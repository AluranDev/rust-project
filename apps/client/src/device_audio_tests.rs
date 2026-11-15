use super::*;
use audio::health::DeviceHealth;
use std::{collections::HashMap, rc::Rc, sync::atomic::AtomicUsize};
struct Gate {
    started: AtomicBool,
    rx: Mutex<Option<sync::Receiver<()>>>,
}
struct Spec {
    rate: u32,
    channels: u16,
    fail: AtomicBool,
    health: DeviceHealth,
    gate: Option<Arc<Gate>>,
}
struct World {
    list: Mutex<Vec<u32>>,
    specs: HashMap<u32, Arc<Spec>>,
    opened: Mutex<Vec<u32>>,
    dropped: Mutex<Vec<u32>>,
    written: Mutex<Vec<(u32, Vec<f32>)>>,
    reads: AtomicUsize,
}
fn world() -> Arc<World> {
    let specs = [(1, 48000, 2), (2, 11025, 1), (3, 44100, 2)]
        .into_iter()
        .map(|(id, rate, channels)| {
            (
                id,
                Arc::new(Spec {
                    rate,
                    channels,
                    fail: AtomicBool::new(false),
                    health: DeviceHealth::default(),
                    gate: None,
                }),
            )
        })
        .collect();
    Arc::new(World {
        list: Mutex::new(vec![1, 2, 3]),
        specs,
        opened: Mutex::new(Vec::new()),
        dropped: Mutex::new(Vec::new()),
        written: Mutex::new(Vec::new()),
        reads: AtomicUsize::new(0),
    })
}
struct Fake {
    world: Arc<World>,
    thread: thread::ThreadId,
}
struct Device {
    id: u32,
    _local: Rc<()>,
}
struct Stream {
    id: u32,
    spec: Arc<Spec>,
    world: Arc<World>,
    thread: thread::ThreadId,
    _local: Rc<()>,
}
impl Drop for Stream {
    fn drop(&mut self) {
        assert_eq!(self.thread, thread::current().id());
        self.world.dropped.lock().unwrap().push(self.id);
    }
}
impl OwnedStream for Stream {
    fn rate(&self) -> u32 {
        self.spec.rate
    }
    fn channels(&self) -> u16 {
        self.spec.channels
    }
    fn check(&self) -> Result<(), String> {
        assert_eq!(self.thread, thread::current().id());
        self.spec.health.check()
    }
    fn read(&mut self, samples: &mut [f32]) -> usize {
        assert_eq!(self.thread, thread::current().id());
        self.world.reads.fetch_add(1, Ordering::SeqCst);
        samples.fill(self.id as f32 / 10.0);
        samples.len()
    }
    fn write(&mut self, samples: &[f32]) -> usize {
        assert_eq!(self.thread, thread::current().id());
        self.world
            .written
            .lock()
            .unwrap()
            .push((self.id, samples.to_vec()));
        samples.len()
    }
    fn discard(&mut self) {
        assert_eq!(self.thread, thread::current().id());
    }
}
impl Backend for Fake {
    type Device = Device;
    type Stream = Stream;
    fn list(&mut self) -> Result<Vec<(String, Device)>, String> {
        assert_eq!(self.thread, thread::current().id());
        Ok(self
            .world
            .list
            .lock()
            .unwrap()
            .iter()
            .map(|id| {
                (
                    "same name".into(),
                    Device {
                        id: *id,
                        _local: Rc::new(()),
                    },
                )
            })
            .collect())
    }
    fn initial(&mut self, _: Option<&str>) -> Result<(String, Device), String> {
        Ok((
            "initial".into(),
            Device {
                id: 1,
                _local: Rc::new(()),
            },
        ))
    }
    fn open(&mut self, device: &Device) -> Result<Stream, String> {
        assert_eq!(self.thread, thread::current().id());
        let spec = self.world.specs[&device.id].clone();
        if let Some(gate) = &spec.gate {
            gate.started.store(true, Ordering::SeqCst);
            if let Some(rx) = gate.rx.lock().unwrap().take() {
                rx.recv().unwrap();
            }
        }
        if spec.fail.load(Ordering::SeqCst) {
            return Err("injected open failure".into());
        }
        spec.health.check()?;
        self.world.opened.lock().unwrap().push(device.id);
        Ok(Stream {
            id: device.id,
            spec,
            world: self.world.clone(),
            thread: self.thread,
            _local: Rc::new(()),
        })
    }
}
async fn start(input: Arc<World>, output: Arc<World>) -> (Devices, InputBridge, OutputBridge) {
    Devices::start_with(
        move || Fake {
            world: input,
            thread: thread::current().id(),
        },
        move || Fake {
            world: output,
            thread: thread::current().id(),
        },
        None,
        None,
        0.5,
    )
    .await
    .unwrap()
}
async fn available(controls: &Controls, direction: Direction) {
    let mut state = controls.state(direction);
    tokio::time::timeout(
        Duration::from_secs(2),
        state.wait_for(|state| matches!(state, DeviceState::Available(_))),
    )
    .await
    .unwrap()
    .unwrap();
}
async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
fn released(world: &World) {
    assert_eq!(
        world.opened.lock().unwrap().len(),
        world.dropped.lock().unwrap().len()
    );
}
#[tokio::test]
async fn retained_handles_distinguish_duplicates_and_do_not_follow_reordered_indices() {
    let input = world();
    let output = world();
    let (devices, mut source, _sink) = start(input.clone(), output.clone()).await;
    let controls = devices.controls();
    available(&controls, Direction::Input).await;
    let list = controls.list(Direction::Input).await.unwrap();
    assert_eq!(list[0].name, list[1].name);
    assert_ne!(list[0].id, list[1].id);
    *input.list.lock().unwrap() = vec![3, 1, 2];
    controls.switch(Direction::Input, list[1].id).await.unwrap();
    assert_eq!(*input.opened.lock().unwrap().last().unwrap(), 2);
    let mut samples = [0.0; 20];
    wait_until(|| source.receive_into(&mut samples) > 0).await;
    assert!(samples.iter().all(|sample| (*sample - 0.2).abs() < 0.00001));
    let _new = controls.list(Direction::Input).await.unwrap();
    assert!(
        controls
            .switch(Direction::Input, list[0].id)
            .await
            .unwrap_err()
            .contains("stale")
    );
    devices.shutdown().await.unwrap();
    released(&input);
    released(&output);
}
#[tokio::test]
async fn failed_open_retains_previous_input_and_output() {
    let input = world();
    let output = world();
    input.specs[&2].fail.store(true, Ordering::SeqCst);
    output.specs[&2].fail.store(true, Ordering::SeqCst);
    let (devices, mut source, mut sink) = start(input.clone(), output.clone()).await;
    let controls = devices.controls();
    for direction in [Direction::Input, Direction::Output] {
        available(&controls, direction).await;
        let list = controls.list(direction).await.unwrap();
        assert!(
            controls
                .switch(direction, list[1].id)
                .await
                .unwrap_err()
                .contains("injected open failure")
        );
        assert!(matches!(
            *controls.state(direction).borrow(),
            DeviceState::Available(_)
        ));
    }
    let mut pcm = [0.0; 20];
    wait_until(|| source.receive_into(&mut pcm) > 0).await;
    assert!(pcm.iter().all(|v| (*v - 0.1).abs() < 0.00001));
    sink.generation();
    assert_eq!(sink.write(&vec![0.8; 1920]), 1920);
    wait_until(|| !output.written.lock().unwrap().is_empty()).await;
    assert_eq!(output.written.lock().unwrap().last().unwrap().0, 1);
    devices.shutdown().await.unwrap();
    released(&input);
    released(&output);
}
#[tokio::test]
async fn repeated_changes_convert_formats_and_keep_volume_and_discard_stale_pcm() {
    let input = world();
    let output = world();
    let (devices, mut source, mut sink) = start(input.clone(), output.clone()).await;
    let controls = devices.controls();
    available(&controls, Direction::Input).await;
    available(&controls, Direction::Output).await;
    let inputs = controls.list(Direction::Input).await.unwrap();
    let outputs = controls.list(Direction::Output).await.unwrap();
    controls.set_volume(0.25).unwrap();
    let mut pcm = [0.0; 20];
    wait_until(|| source.receive_into(&mut pcm) > 0).await;
    let old_epoch = source.generation();
    for index in [1, 2, 0, 1] {
        controls
            .switch(Direction::Input, inputs[index].id)
            .await
            .unwrap();
        controls
            .switch(Direction::Output, outputs[index].id)
            .await
            .unwrap();
        assert_eq!(controls.volume(), 0.25);
        wait_until(|| source.receive_into(&mut pcm) > 0).await;
        assert!(
            pcm.iter()
                .all(|v| (*v - (index + 1) as f32 / 10.0).abs() < 0.00001)
        );
        let before = output.written.lock().unwrap().len();
        assert_eq!(
            sink.write(&vec![0.9; 1920]),
            0,
            "stale output epoch was accepted"
        );
        sink.generation();
        sink.write(&vec![0.8; 1920]);
        wait_until(|| output.written.lock().unwrap().len() > before).await;
        let log = output.written.lock().unwrap();
        let (id, samples) = log.last().unwrap();
        assert_eq!(*id, (index + 1) as u32);
        assert!(samples.iter().all(|v| (*v - 0.2).abs() < 0.00001));
        let expected = if index == 1 {
            220
        } else if index == 2 {
            1764
        } else {
            1920
        };
        assert_eq!(samples.len(), expected);
    }
    assert!(!source.is_current(old_epoch));
    devices.shutdown().await.unwrap();
    released(&input);
    released(&output);
}
#[tokio::test]
async fn unplugged_input_and_output_can_be_replaced_without_stopping_bridges() {
    let input = world();
    let output = world();
    let (devices, mut source, mut sink) = start(input.clone(), output.clone()).await;
    let controls = devices.controls();
    available(&controls, Direction::Input).await;
    available(&controls, Direction::Output).await;
    input.specs[&1].health.fail("input unplugged".into());
    output.specs[&1].health.fail("output unplugged".into());
    for direction in [Direction::Input, Direction::Output] {
        let mut state = controls.state(direction);
        tokio::time::timeout(
            Duration::from_secs(2),
            state.wait_for(|v| matches!(v, DeviceState::Unavailable(_))),
        )
        .await
        .unwrap()
        .unwrap();
    }
    assert!(source.check_health().is_ok() && sink.check_health().is_ok());
    let mut pcm = [0.0; 20];
    assert_eq!(source.receive_into(&mut pcm), 0);
    sink.generation();
    assert_eq!(sink.write(&[0.5; 20]), 0);
    for direction in [Direction::Input, Direction::Output] {
        let entries = controls.list(direction).await.unwrap();
        controls.switch(direction, entries[1].id).await.unwrap();
        available(&controls, direction).await;
    }
    wait_until(|| source.receive_into(&mut pcm) > 0).await;
    sink.generation();
    assert_eq!(sink.write(&vec![0.8; 1920]), 1920);
    wait_until(|| !output.written.lock().unwrap().is_empty()).await;
    devices.shutdown().await.unwrap();
    released(&input);
    released(&output);
}
#[tokio::test]
async fn pending_native_open_does_not_block_other_audio_and_close_cancels_commit() {
    let mut input = world();
    let output = world();
    let (release, rx) = sync::channel();
    let gate = Arc::new(Gate {
        started: AtomicBool::new(false),
        rx: Mutex::new(Some(rx)),
    });
    Arc::get_mut(&mut input).unwrap().specs.insert(
        2,
        Arc::new(Spec {
            rate: 11025,
            channels: 1,
            fail: AtomicBool::new(false),
            health: DeviceHealth::default(),
            gate: Some(gate.clone()),
        }),
    );
    let (devices, _source, mut sink) = start(input.clone(), output.clone()).await;
    let controls = devices.controls();
    available(&controls, Direction::Input).await;
    available(&controls, Direction::Output).await;
    let list = controls.list(Direction::Input).await.unwrap();
    let change_controls = controls.clone();
    let change =
        tokio::spawn(async move { change_controls.switch(Direction::Input, list[1].id).await });
    wait_until(|| gate.started.load(Ordering::SeqCst)).await;
    sink.generation();
    sink.write(&vec![0.8; 1920]);
    wait_until(|| !output.written.lock().unwrap().is_empty()).await;
    controls.set_volume(0.4).unwrap();
    assert_eq!(controls.volume(), 0.4);
    let stopped = devices.stop.clone();
    let closing = tokio::spawn(devices.shutdown());
    wait_until(|| stopped.load(Ordering::SeqCst)).await;
    assert!(!closing.is_finished());
    release.send(()).unwrap();
    assert!(change.await.unwrap().unwrap_err().contains("cancelled"));
    closing.await.unwrap().unwrap();
    assert!(matches!(
        *controls.state(Direction::Input).borrow(),
        DeviceState::Closed
    ));
    released(&input);
    released(&output);
}

#[tokio::test]
async fn device_changes_keep_the_negotiated_peer_and_mute_with_real_rtp() {
    use rtc::{
        audio_receiver::AudioReceiver,
        audio_sender::AudioSenderWorker,
        signaling_client::{SessionCommand, SessionState, SignalingClient},
    };
    use tokio::sync::{oneshot, watch};
    tokio::task::LocalSet::new()
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}/ws", listener.local_addr().unwrap());
            let state = signaling_server::ServerState::default();
            let app = signaling_server::router(state.clone());
            let (shutdown, stopped) = oneshot::channel();
            let server = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
            let (a, b) = tokio::join!(
                SignalingClient::connect(&url, "devices", "alice", 1234),
                SignalingClient::connect(&url, "devices", "bob", 5678)
            );
            let (a, mut b) = (a.unwrap(), b.unwrap());
            for client in [&a, &b] {
                let mut state = client.state.clone();
                tokio::time::timeout(
                    Duration::from_secs(5),
                    state.wait_for(|value| matches!(value, SessionState::Connected)),
                )
                .await
                .unwrap()
                .unwrap();
            }
            let peer = a.peer.clone();
            let original = Arc::as_ptr(&peer);
            a.command(SessionCommand::SetMuted(true)).await.unwrap();
            let input = world();
            let output = world();
            let (devices, source, _sink) = start(input.clone(), output.clone()).await;
            let controls = devices.controls();
            available(&controls, Direction::Input).await;
            available(&controls, Direction::Output).await;
            let inputs = controls.list(Direction::Input).await.unwrap();
            let outputs = controls.list(Direction::Output).await.unwrap();
            let (stop, stopped) = watch::channel(false);
            let sender = tokio::task::spawn_local(
                AudioSenderWorker::new(source, peer.clone(), 20)
                    .unwrap()
                    .run(stopped),
            );
            controls.set_volume(0.35).unwrap();
            for index in [1, 2, 0] {
                controls
                    .switch(Direction::Input, inputs[index].id)
                    .await
                    .unwrap();
                controls
                    .switch(Direction::Output, outputs[index].id)
                    .await
                    .unwrap();
            }
            assert_eq!(controls.volume(), 0.35);
            assert_eq!(Arc::as_ptr(&a.peer), original);
            assert_eq!(*a.state.borrow(), SessionState::Connected);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), b.tracks.recv())
                    .await
                    .is_err(),
                "changing devices lost mute"
            );
            a.command(SessionCommand::SetMuted(false)).await.unwrap();
            let (remote, track) = tokio::time::timeout(Duration::from_secs(3), b.tracks.recv())
                .await
                .unwrap()
                .unwrap();
            let (tx, mut rx) = mpsc::channel(8);
            let receiver = tokio::spawn(
                AudioReceiver::new(track, remote, tx, 48000, 2, 20)
                    .unwrap()
                    .run(),
            );
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .samples
                    .len(),
                1920
            );
            controls
                .switch(Direction::Input, inputs[1].id)
                .await
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .samples
                    .len(),
                1920
            );
            assert_eq!(Arc::as_ptr(&a.peer), original);
            assert_eq!(*b.state.borrow(), SessionState::Connected);
            stop.send(true).unwrap();
            sender.await.unwrap().unwrap();
            devices.shutdown().await.unwrap();
            a.close().await.unwrap();
            b.close().await.unwrap();
            drop(peer);
            let stats = tokio::time::timeout(Duration::from_secs(2), receiver)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stats.decode_errors, 0);
            wait_until(|| state.active_connections() == 0).await;
            assert_eq!(state.room_count().await, 0);
            shutdown.send(()).unwrap();
            server.await.unwrap();
            released(&input);
            released(&output);
        })
        .await;
}

//! CPAL streams never leave their owner thread. The RTC workers see bounded PCM bridges.
use audio::{capture::AudioCapture, format::PcmResampler, playback::AudioPlayback};
use rtc::{audio_sender::CaptureSource, playback_worker::PlaybackSink};
use std::{
    cell::Cell,
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc as sync,
    },
    thread,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

const CAPACITY: usize = 48000 * 2 * 80 / 1000;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Input,
    Output,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceId {
    pub snapshot: u64,
    pub index: usize,
}
impl std::fmt::Display for DeviceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.snapshot, self.index)
    }
}
impl std::str::FromStr for DeviceId {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, String> {
        let (snapshot, index) = value
            .split_once(':')
            .ok_or("Use the ID printed by devices (LIST:INDEX)")?;
        Ok(Self {
            snapshot: snapshot.parse().map_err(|_| "Invalid list ID")?,
            index: index.parse().map_err(|_| "Invalid device index")?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub id: DeviceId,
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceState {
    Opening,
    Available(Entry),
    Changing(DeviceId),
    Unavailable(String),
    Closed,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceEvent {
    State(Direction, DeviceState),
    ChangeFailed(Direction, String),
}
#[derive(Default)]
struct PcmQueue {
    samples: VecDeque<f32>,
    epoch: u64,
    active: bool,
    closed: bool,
    dropped: u64,
}
type Queue = Arc<Mutex<PcmQueue>>;
fn lock(queue: &Queue) -> std::sync::MutexGuard<'_, PcmQueue> {
    queue.lock().unwrap_or_else(|e| e.into_inner())
}
fn clear(queue: &Queue, active: bool) {
    let mut q = lock(queue);
    q.samples.clear();
    q.epoch += 1;
    q.active = active;
}
fn push(queue: &mut PcmQueue, samples: &[f32]) -> usize {
    let count = if queue.active {
        (CAPACITY - queue.samples.len()).min(samples.len()) / 2 * 2
    } else {
        0
    };
    queue.samples.extend(&samples[..count]);
    queue.dropped += (samples.len() - count) as u64;
    count
}
pub struct InputBridge {
    queue: Queue,
    observed: Cell<u64>,
}
impl CaptureSource for InputBridge {
    fn sample_rate(&self) -> u32 {
        48000
    }
    fn channels(&self) -> u16 {
        2
    }
    fn receive_into(&mut self, out: &mut [f32]) -> usize {
        let mut q = lock(&self.queue);
        self.observed.set(q.epoch);
        let count = out.len().min(q.samples.len()) / 2 * 2;
        for value in &mut out[..count] {
            *value = q.samples.pop_front().unwrap();
        }
        count
    }
    fn generation(&self) -> u64 {
        self.observed.get()
    }
    fn is_current(&self, generation: u64) -> bool {
        let q = lock(&self.queue);
        q.epoch == generation && q.active
    }
    fn check_health(&self) -> Result<(), String> {
        if lock(&self.queue).closed {
            Err("input owner stopped".into())
        } else {
            Ok(())
        }
    }
    fn take_backend_errors(&self) -> u64 {
        0
    }
    fn take_dropped_samples(&self) -> u64 {
        let mut q = lock(&self.queue);
        std::mem::take(&mut q.dropped)
    }
}
pub struct OutputBridge {
    queue: Queue,
    observed: Cell<u64>,
}
impl PlaybackSink for OutputBridge {
    fn sample_rate(&self) -> u32 {
        48000
    }
    fn channels(&self) -> u16 {
        2
    }
    fn generation(&self) -> u64 {
        let epoch = lock(&self.queue).epoch;
        self.observed.set(epoch);
        epoch
    }
    fn check_health(&self) -> Result<(), String> {
        if lock(&self.queue).closed {
            Err("output owner stopped".into())
        } else {
            Ok(())
        }
    }
    fn write(&mut self, samples: &[f32]) -> usize {
        let mut q = lock(&self.queue);
        if self.observed.get() != q.epoch {
            self.observed.set(q.epoch);
            q.dropped += samples.len() as u64;
            return 0;
        }
        push(&mut q, samples)
    }
    fn take_backend_errors(&self) -> u64 {
        0
    }
    fn take_underrun_callbacks(&self) -> u64 {
        0
    }
    fn take_dropped_samples(&mut self) -> u64 {
        let mut q = lock(&self.queue);
        std::mem::take(&mut q.dropped)
    }
}
// The backend and its device/stream objects are constructed on, and stay on,
// the owner thread. Associated types intentionally do not require Send.
pub trait OwnedStream {
    fn rate(&self) -> u32;
    fn channels(&self) -> u16;
    fn check(&self) -> Result<(), String>;
    fn read(&mut self, _samples: &mut [f32]) -> usize {
        0
    }
    fn write(&mut self, _samples: &[f32]) -> usize {
        0
    }
    fn discard(&mut self);
}
pub trait Backend {
    type Device;
    type Stream: OwnedStream;
    fn list(&mut self) -> Result<Vec<(String, Self::Device)>, String>;
    fn initial(&mut self, name: Option<&str>) -> Result<(String, Self::Device), String>;
    fn open(&mut self, device: &Self::Device) -> Result<Self::Stream, String>;
}
struct CpalBackend(Direction);
enum Native {
    Input(AudioCapture),
    Output(AudioPlayback),
}
impl OwnedStream for Native {
    fn rate(&self) -> u32 {
        match self {
            Self::Input(s) => s.sample_rate(),
            Self::Output(s) => s.sample_rate(),
        }
    }
    fn channels(&self) -> u16 {
        match self {
            Self::Input(s) => s.channels(),
            Self::Output(s) => s.channels(),
        }
    }
    fn check(&self) -> Result<(), String> {
        match self {
            Self::Input(s) => s.check_health(),
            Self::Output(s) => s.check_health(),
        }
    }
    fn read(&mut self, samples: &mut [f32]) -> usize {
        match self {
            Self::Input(s) => s.receive_into(samples),
            _ => 0,
        }
    }
    fn write(&mut self, samples: &[f32]) -> usize {
        match self {
            Self::Output(s) => s.write(samples),
            _ => 0,
        }
    }
    fn discard(&mut self) {
        match self {
            Self::Input(s) => s.discard_pending(),
            Self::Output(s) => s.discard_pending(),
        }
    }
}
impl Backend for CpalBackend {
    type Device = cpal::Device;
    type Stream = Native;
    fn list(&mut self) -> Result<Vec<(String, cpal::Device)>, String> {
        use cpal::traits::{DeviceTrait, HostTrait};
        let host = cpal::default_host();
        let devices = match self.0 {
            Direction::Input => host.input_devices(),
            Direction::Output => host.output_devices(),
        }
        .map_err(|e| e.to_string())?;
        Ok(devices
            .map(|device| {
                let name = device
                    .description()
                    .map(|d| d.name().to_owned())
                    .unwrap_or_else(|_| "Unknown".into());
                (name, device)
            })
            .collect())
    }
    fn initial(&mut self, name: Option<&str>) -> Result<(String, cpal::Device), String> {
        use cpal::traits::DeviceTrait;
        let device = match self.0 {
            Direction::Input => audio::devices::select_input(name),
            Direction::Output => audio::devices::select_output(name),
        }?;
        let name = device
            .description()
            .map(|d| d.name().to_owned())
            .map_err(|e| e.to_string())?;
        Ok((name, device))
    }
    fn open(&mut self, device: &cpal::Device) -> Result<Native, String> {
        match self.0 {
            Direction::Input => AudioCapture::new(device.clone()).map(Native::Input),
            Direction::Output => AudioPlayback::new(device.clone()).map(Native::Output),
        }
    }
}
enum Request {
    List(oneshot::Sender<Result<Vec<Entry>, String>>),
    Switch(DeviceId, oneshot::Sender<Result<(), String>>),
}
#[derive(Clone)]
struct Port {
    tx: sync::SyncSender<Request>,
    state: watch::Receiver<DeviceState>,
}
pub struct Devices {
    input: Port,
    output: Port,
    stop: Arc<AtomicBool>,
    volume: Arc<AtomicU32>,
    handles: Vec<thread::JoinHandle<()>>,
    pub events: mpsc::Receiver<DeviceEvent>,
}
impl Drop for Devices {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}
impl Devices {
    pub async fn start(
        input: Option<String>,
        output: Option<String>,
        volume: f32,
    ) -> Result<(Self, InputBridge, OutputBridge), String> {
        Self::start_with(
            || CpalBackend(Direction::Input),
            || CpalBackend(Direction::Output),
            input,
            output,
            volume,
        )
        .await
    }
    pub async fn start_with<FI, FO, BI, BO>(
        input: FI,
        output: FO,
        input_name: Option<String>,
        output_name: Option<String>,
        volume: f32,
    ) -> Result<(Self, InputBridge, OutputBridge), String>
    where
        FI: FnOnce() -> BI + Send + 'static,
        FO: FnOnce() -> BO + Send + 'static,
        BI: Backend + 'static,
        BO: Backend + 'static,
    {
        if !volume.is_finite() || !(0.0..=1.0).contains(&volume) {
            return Err("volume must be between 0 and 1".into());
        }
        let stop = Arc::new(AtomicBool::new(false));
        let volume = Arc::new(AtomicU32::new(volume.to_bits()));
        let iq = Queue::default();
        let oq = Queue::default();
        let (events_tx, events) = mpsc::channel(64);
        let (itx, irx) = sync::sync_channel(8);
        let (otx, orx) = sync::sync_channel(8);
        let (istx, istate) = watch::channel(DeviceState::Opening);
        let (ostx, ostate) = watch::channel(DeviceState::Opening);
        let mut handles = Vec::new();
        let context = Owner {
            direction: Direction::Input,
            queue: iq.clone(),
            stop: stop.clone(),
            volume: volume.clone(),
            events: events_tx.clone(),
            state: istx,
        };
        handles.push(
            thread::Builder::new()
                .name("audio-input".into())
                .spawn(move || owner(input(), context, irx, input_name))
                .map_err(|e| e.to_string())?,
        );
        let context = Owner {
            direction: Direction::Output,
            queue: oq.clone(),
            stop: stop.clone(),
            volume: volume.clone(),
            events: events_tx,
            state: ostx,
        };
        match thread::Builder::new()
            .name("audio-output".into())
            .spawn(move || owner(output(), context, orx, output_name))
        {
            Ok(handle) => handles.push(handle),
            Err(error) => {
                stop.store(true, Ordering::SeqCst);
                let _ = tokio::task::spawn_blocking(move || {
                    for handle in handles {
                        let _ = handle.join();
                    }
                })
                .await;
                return Err(error.to_string());
            }
        }
        Ok((
            Self {
                input: Port {
                    tx: itx,
                    state: istate,
                },
                output: Port {
                    tx: otx,
                    state: ostate,
                },
                stop,
                volume,
                handles,
                events,
            },
            InputBridge {
                queue: iq,
                observed: Cell::new(0),
            },
            OutputBridge {
                queue: oq,
                observed: Cell::new(0),
            },
        ))
    }
    pub fn controls(&self) -> Controls {
        Controls {
            input: self.input.clone(),
            output: self.output.clone(),
            volume: self.volume.clone(),
        }
    }
    pub fn request_shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }
    pub async fn shutdown(mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::SeqCst);
        let handles = std::mem::take(&mut self.handles);
        tokio::task::spawn_blocking(move || {
            let mut failed = false;
            for handle in handles {
                if handle.join().is_err() {
                    failed = true;
                }
            }
            if failed {
                Err("device owner panicked".to_string())
            } else {
                Ok(())
            }
        })
        .await
        .map_err(|e| e.to_string())?
    }
}
#[derive(Clone)]
pub struct Controls {
    input: Port,
    output: Port,
    volume: Arc<AtomicU32>,
}
impl Controls {
    fn port(&self, direction: Direction) -> &Port {
        match direction {
            Direction::Input => &self.input,
            Direction::Output => &self.output,
        }
    }
    pub fn state(&self, direction: Direction) -> watch::Receiver<DeviceState> {
        self.port(direction).state.clone()
    }
    pub async fn list(&self, direction: Direction) -> Result<Vec<Entry>, String> {
        let (tx, rx) = oneshot::channel();
        self.port(direction)
            .tx
            .try_send(Request::List(tx))
            .map_err(|_| "device owner busy or closed")?;
        rx.await.map_err(|_| "device owner closed".to_string())?
    }
    pub async fn switch(&self, direction: Direction, id: DeviceId) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.port(direction)
            .tx
            .try_send(Request::Switch(id, tx))
            .map_err(|_| "device owner busy or closed")?;
        rx.await.map_err(|_| "device owner closed".to_string())?
    }
    pub fn set_volume(&self, volume: f32) -> Result<(), String> {
        if !volume.is_finite() || !(0.0..=1.0).contains(&volume) {
            return Err("volume must be between 0 and 1".into());
        }
        self.volume.store(volume.to_bits(), Ordering::Relaxed);
        Ok(())
    }
    pub fn volume(&self) -> f32 {
        f32::from_bits(self.volume.load(Ordering::Relaxed))
    }
}
struct Owner {
    direction: Direction,
    queue: Queue,
    stop: Arc<AtomicBool>,
    volume: Arc<AtomicU32>,
    events: mpsc::Sender<DeviceEvent>,
    state: watch::Sender<DeviceState>,
}
impl Owner {
    fn publish(&self, state: DeviceState) {
        self.state.send_replace(state.clone());
        let _ = self
            .events
            .try_send(DeviceEvent::State(self.direction, state));
    }
    fn failed(&self, error: String) {
        let _ = self
            .events
            .try_send(DeviceEvent::ChangeFailed(self.direction, error));
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let mut q = lock(&self.queue);
        q.samples.clear();
        q.active = false;
        q.closed = true;
        drop(q);
        self.publish(DeviceState::Closed);
    }
}
struct Active<S> {
    stream: S,
    resampler: PcmResampler,
    entry: Entry,
    buffer: Vec<f32>,
}
fn prepare<S: OwnedStream>(
    mut stream: S,
    entry: Entry,
    direction: Direction,
) -> Result<Active<S>, String> {
    if stream.rate() == 0 || ![1, 2].contains(&stream.channels()) {
        return Err("device must support a nonzero rate and mono or stereo".into());
    }
    stream.check()?;
    let resampler = match direction {
        Direction::Input => PcmResampler::new(stream.rate(), stream.channels(), 48000, 2),
        Direction::Output => PcmResampler::new(48000, 2, stream.rate(), stream.channels()),
    }?;
    let size = match direction {
        Direction::Input => {
            (stream.rate() as usize * 20 / 1000).max(1) * stream.channels() as usize
        }
        Direction::Output => 1920,
    };
    stream.discard();
    Ok(Active {
        stream,
        resampler,
        entry,
        buffer: vec![0.0; size],
    })
}
fn owner<B: Backend>(
    mut backend: B,
    context: Owner,
    requests: sync::Receiver<Request>,
    initial_name: Option<String>,
) {
    let mut snapshot = 0;
    let mut listed = Vec::new();
    let initial = backend
        .initial(initial_name.as_deref())
        .and_then(|(name, device)| {
            backend.open(&device).and_then(|stream| {
                prepare(
                    stream,
                    Entry {
                        id: DeviceId {
                            snapshot: 0,
                            index: 0,
                        },
                        name,
                    },
                    context.direction,
                )
            })
        });
    let mut active = match initial {
        Ok(value) if !context.stop.load(Ordering::SeqCst) => {
            clear(&context.queue, true);
            context.publish(DeviceState::Available(value.entry.clone()));
            Some(value)
        }
        Ok(_) => None,
        Err(error) => {
            context.publish(DeviceState::Unavailable(error));
            None
        }
    };
    while !context.stop.load(Ordering::SeqCst) {
        match requests.recv_timeout(Duration::from_millis(2)) {
            Ok(Request::List(reply)) => {
                let result = backend.list().map(|devices| {
                    snapshot += 1;
                    listed = devices;
                    listed
                        .iter()
                        .enumerate()
                        .map(|(index, (name, _))| Entry {
                            id: DeviceId { snapshot, index },
                            name: name.clone(),
                        })
                        .collect()
                });
                let _ = reply.send(result);
            }
            Ok(Request::Switch(id, reply)) => {
                let Some((name, device)) = listed.get(id.index).filter(|_| id.snapshot == snapshot)
                else {
                    let _ = reply.send(Err("stale or unknown device ID; run devices again".into()));
                    continue;
                };
                context.publish(DeviceState::Changing(id));
                let result = backend.open(device).and_then(|stream| {
                    prepare(
                        stream,
                        Entry {
                            id,
                            name: name.clone(),
                        },
                        context.direction,
                    )
                });
                if context.stop.load(Ordering::SeqCst) || reply.is_closed() {
                    drop(result);
                    if !context.stop.load(Ordering::SeqCst) {
                        if let Some(old) = active.as_mut() {
                            old.stream.discard();
                            clear(&context.queue, true);
                            context.publish(DeviceState::Available(old.entry.clone()));
                        } else {
                            context.publish(DeviceState::Unavailable(
                                "device change cancelled".into(),
                            ));
                        }
                    }
                    let _ = reply.send(Err("device change cancelled".into()));
                    continue;
                }
                match result {
                    Ok(mut value) => {
                        // Releasing the old backend may take time. Discard the new
                        // stream's samples after that release, just before activation.
                        clear(&context.queue, false);
                        drop(active.take());
                        if context.stop.load(Ordering::SeqCst) || reply.is_closed() {
                            if !context.stop.load(Ordering::SeqCst) {
                                context.publish(DeviceState::Unavailable(
                                    "device change cancelled during release; select a device"
                                        .into(),
                                ));
                            }
                            let _ = reply.send(Err("device change cancelled".into()));
                            continue;
                        }
                        value.stream.discard();
                        clear(&context.queue, true);
                        active = Some(value);
                        context.publish(DeviceState::Available(
                            active.as_ref().unwrap().entry.clone(),
                        ));
                        let _ = reply.send(Ok(()));
                    }
                    Err(error) => {
                        if let Some(old) = active.as_mut() {
                            old.stream.discard();
                            clear(&context.queue, true);
                            context.publish(DeviceState::Available(old.entry.clone()));
                        } else {
                            context.publish(DeviceState::Unavailable(error.clone()));
                        }
                        context.failed(error.clone());
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Err(sync::RecvTimeoutError::Disconnected) => break,
            Err(sync::RecvTimeoutError::Timeout) => (),
        }
        if context.stop.load(Ordering::SeqCst) {
            break;
        }
        let failure = if let Some(device) = active.as_mut() {
            device
                .stream
                .check()
                .and_then(|()| match context.direction {
                    Direction::Input => {
                        let count = device.stream.read(&mut device.buffer);
                        let samples = device.resampler.process(&device.buffer[..count])?;
                        push(&mut lock(&context.queue), &samples);
                        Ok(())
                    }
                    Direction::Output => {
                        let count = {
                            let mut q = lock(&context.queue);
                            let count = q.samples.len().min(device.buffer.len()) / 2 * 2;
                            for value in &mut device.buffer[..count] {
                                *value = q.samples.pop_front().unwrap();
                            }
                            count
                        };
                        if count > 0 {
                            let mut samples = device.resampler.process(&device.buffer[..count])?;
                            let volume = f32::from_bits(context.volume.load(Ordering::Relaxed));
                            for sample in &mut samples {
                                *sample *= volume;
                            }
                            device.stream.write(&samples);
                        }
                        Ok(())
                    }
                })
                .err()
        } else {
            None
        };
        if let Some(error) = failure {
            active = None;
            clear(&context.queue, false);
            context.publish(DeviceState::Unavailable(error));
        }
    }
    // Stream drop happens before the Closed event and on this same owner thread.
    drop(active);
}

#[cfg(test)]
#[path = "device_audio_tests.rs"]
mod tests;

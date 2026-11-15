use audio::codec::opus::OpusCodec;
use rtc::{
    audio_receiver::AudioReceiver,
    signaling_client::{JoinSession, SessionState, SignalingClient},
};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
type Error = Box<dyn std::error::Error + Send + Sync>;

pub async fn synthetic(peer_id: &str, room: &str) -> Result<(), Error> {
    let url =
        std::env::var("VOICE_SIGNALING_URL").unwrap_or_else(|_| "ws://127.0.0.1:3000/ws".into());
    let mut config = JoinSession::local(
        &url,
        room,
        peer_id,
        if peer_id == "alice" { 1234 } else { 5678 },
    );
    if let Ok(token) = std::env::var("VOICE_SIGNALING_TOKEN") {
        config.token = token;
    }
    let mut client = SignalingClient::join(config).await?;
    let mut state = client.state.clone();
    let connected: Result<(), Error> = async {
        loop {
            match state.borrow_and_update().clone() {
                SessionState::Connected => return Ok(()),
                SessionState::Failed(error) => return Err(error.into()),
                SessionState::Closed => return Err("call closed before connection".into()),
                _ => (),
            }
            state.changed().await?;
        }
    }
    .await;
    if let Err(error) = connected {
        let _ = client.close().await;
        return Err(error);
    }
    println!(
        "Connected | negotiated {:?}",
        client.peer.negotiated_audio()
    );
    let peer = client.peer.clone();
    let (stop, mut stopped) = watch::channel(false);
    let sender = tokio::spawn(async move {
        let mut codec = OpusCodec::new(48000, 2, 20)?;
        let mut packet = vec![0; 4000];
        let mut index = 0u64;
        loop {
            let samples: Vec<f32> = (0..960)
                .flat_map(|i| {
                    let s =
                        (std::f32::consts::TAU * 440.0 * (index + i) as f32 / 48000.0).sin() * 0.2;
                    [s, s]
                })
                .collect();
            index += 960;
            let n = codec.encode_frame(&samples, &mut packet)?;
            peer.send_opus(&packet[..n], Duration::from_millis(20))
                .await
                .map_err(|error| error.to_string())?;
            tokio::select! { _ = stopped.changed() => break, _ = tokio::time::sleep(Duration::from_millis(20)) => () }
        }
        Ok::<(), String>(())
    });
    let mut receiver: Option<tokio::task::JoinHandle<rtc::audio_receiver::AudioReceiverStats>> =
        None;
    let result: Result<(), Error> = async {
        let (remote, track) = tokio::time::timeout(Duration::from_secs(5), client.tracks.recv())
            .await?
            .ok_or("remote track closed")?;
        let (tx, mut rx) = mpsc::channel(8);
        receiver = Some(tokio::spawn(
            AudioReceiver::new(track, remote, tx, 48000, 2, 20)
                .map_err(std::io::Error::other)?
                .run(),
        ));
        for _ in 0..50 {
            let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await?
                .ok_or("audio closed")?;
            if frame.samples.len() != 1920
                || !frame.samples.iter().all(|s| s.is_finite())
                || !frame.samples.iter().any(|s| s.abs() > 0.00001)
            {
                return Err("invalid or silent decoded audio".into());
            }
            println!(
                "Decoded {} samples from {}",
                frame.samples.len(),
                frame.peer_id
            );
        }
        Ok(())
    }
    .await;
    let _ = stop.send(true);
    let sent = sender.await?;
    let closed = client.close().await;
    if let Some(receiver) = receiver {
        let stats = tokio::time::timeout(Duration::from_secs(3), receiver).await??;
        println!("Receiver stopped naturally: {stats:?}");
        if stats.incompatible_packets > 0 || stats.decode_errors > 0 {
            return Err("incompatible or undecodable audio".into());
        }
    }
    result?;
    sent.map_err(std::io::Error::other)?;
    closed
}

/// Native streams are owned by the device threads; this task only supervises
/// RTC workers, console commands, and asynchronous device requests.
pub async fn voice(peer_id: &str, room: &str) -> Result<(), Error> {
    use crate::{
        console::Console,
        device_audio::{Devices, Direction},
    };
    use rtc::{audio_sender::AudioSenderWorker, playback_worker::PlaybackWorker};
    let volume: f32 = std::env::var("VOICE_VOLUME")
        .unwrap_or_else(|_| "1.0".into())
        .parse()?;
    if !volume.is_finite() || !(0.0..=1.0).contains(&volume) {
        return Err("volume must be between 0 and 1".into());
    }
    let input_name = std::env::var("VOICE_INPUT_DEVICE").ok();
    let output_name = std::env::var("VOICE_OUTPUT_DEVICE").ok();
    let url =
        std::env::var("VOICE_SIGNALING_URL").unwrap_or_else(|_| "ws://127.0.0.1:3000/ws".into());
    let mut config = JoinSession::local(
        &url,
        room,
        peer_id,
        if peer_id == "alice" { 1234 } else { 5678 },
    );
    if let Ok(token) = std::env::var("VOICE_SIGNALING_TOKEN") {
        config.token = token;
    }
    let mut client = tokio::select! {client=SignalingClient::join(config)=>client?,signal=tokio::signal::ctrl_c()=>{signal?;return Ok(());}};
    let mut console = match Console::start() {
        Ok(reader) => reader,
        Err(error) => {
            let _ = client.close().await;
            return Err(error.into());
        }
    };
    let mut state = client.state.clone();
    let (stop, stopped) = watch::channel(false);
    let mut sender: Option<
        tokio::task::JoinHandle<Result<rtc::audio_sender::AudioSenderStats, String>>,
    > = None;
    let mut receiver: Option<tokio::task::JoinHandle<rtc::audio_receiver::AudioReceiverStats>> =
        None;
    let mut player: Option<
        tokio::task::JoinHandle<Result<rtc::playback_worker::PlaybackWorkerStats, String>>,
    > = None;
    let mut devices = None;
    let mut commands_open = true;
    let outcome:Result<(),Error>=async{
        loop{
            match state.borrow_and_update().clone(){SessionState::Connected=>break,SessionState::Failed(error)=>return Err(error.into()),SessionState::Closed=>return Ok(()),_=>()}
            tokio::select!{
                changed=state.changed()=>{changed?;},
                signal=tokio::signal::ctrl_c()=>{signal?;return Ok(());},
                command=console.lines.recv(),if commands_open=>match command{None=>commands_open=false,Some(Ok(line))if line.trim()=="quit"=>return Ok(()),_=>println!("Waiting for peer connection; use quit to leave")},
            }
        }
        // No CPAL stream is opened before the peer is connected.
        let (manager,input,output)=Devices::start(input_name,output_name,volume).await.map_err(std::io::Error::other)?;
        let controls=manager.controls();devices=Some(manager);
        sender=Some(tokio::task::spawn_local(AudioSenderWorker::new(input,client.peer.clone(),20).map_err(std::io::Error::other)?.run(stopped.clone())));
        let mut output=Some(output);let mut pending:Option<DeviceAction>=None;
        println!("Commands: devices, input LIST:INDEX, output LIST:INDEX, mute, unmute, volume 0..1, status, quit");
        loop{
            tokio::select!{
                command=console.lines.recv(),if commands_open=>{
                    match command{
                        None=>commands_open=false,
                        Some(Err(error))=>{eprintln!("Console unavailable: {error}; Ctrl+C still closes the call");commands_open=false;},
                        Some(Ok(line))=>{
                            let mut parts=line.split_whitespace();
                            match parts.next(){
                                Some("quit")=>return Ok(()),
                                Some("mute")=>{client.command(rtc::signaling_client::SessionCommand::SetMuted(true)).await?;println!("Muted");},
                                Some("unmute")=>{client.command(rtc::signaling_client::SessionCommand::SetMuted(false)).await?;println!("Unmuted");},
                                Some("volume")=>{
                                    let value=parts.next().and_then(|value|value.parse::<f32>().ok());
                                    match value{Some(value)=>if let Err(error)=controls.set_volume(value){eprintln!("{error}");},None=>eprintln!("Usage: volume 0..1")}
                                },
                                Some("status")=>println!("Input: {:?} | Output: {:?} | Volume: {}",*controls.state(Direction::Input).borrow(),*controls.state(Direction::Output).borrow(),controls.volume()),
                                Some("devices")=>{
                                    if pending.is_some(){eprintln!("A device operation is pending; status, mute, volume and quit remain available");continue;}
                                    let controls=controls.clone();pending=Some(Box::pin(async move{let input=controls.list(Direction::Input).await?;let output=controls.list(Direction::Output).await?;Ok(DeviceResult::Listed(input,output))}));
                                },
                                Some(kind @ ("input"|"output"))=>{
                                    if pending.is_some(){eprintln!("A device operation is pending; use quit to close");continue;}
                                    let id=match parts.next().ok_or("Missing ID").and_then(|value|value.parse::<crate::device_audio::DeviceId>().map_err(|_|"Use LIST:INDEX from devices")){Ok(id)=>id,Err(error)=>{eprintln!("{error}");continue;}};
                                    let direction=if kind=="input"{Direction::Input}else{Direction::Output};let controls=controls.clone();
                                    pending=Some(Box::pin(async move{controls.switch(direction,id).await?;Ok(DeviceResult::Changed(direction,id))}));
                                },
                                Some(_)=>eprintln!("Commands: devices, input LIST:INDEX, output LIST:INDEX, mute, unmute, volume 0..1, status, quit"),None=>(),
                            }
                        }
                    }
                },
                result=async{pending.as_mut().unwrap().await},if pending.is_some()=>{
                    pending=None;
                    match result{
                        Ok(DeviceResult::Listed(input,output))=>for (direction,entries)in [(Direction::Input,input),(Direction::Output,output)]{for entry in entries{println!("{direction:?} {}: {}",entry.id,entry.name);}},
                        Ok(DeviceResult::Changed(direction,id))=>println!("{direction:?} changed to {id}"),Err(error)=>eprintln!("Device operation failed: {error}"),
                    }
                },
                event=devices.as_mut().unwrap().events.recv()=>{
                    match event{Some(event)=>println!("Device: {event:?}"),None=>return Err("device owner event channel closed".into())}
                },
                signal=tokio::signal::ctrl_c()=>{signal?;return Ok(());},
                changed=state.changed()=>{
                    changed?;
                    let value=state.borrow_and_update().clone();
                    match value{SessionState::Failed(error)=>return Err(error.into()),SessionState::Closed=>return Ok(()),SessionState::Closing=>{let _=stop.send(true);return worker_finished(&mut state,"session",Ok(())).await.map_err(Into::into);},_=>()}
                },
                result=async{sender.as_mut().unwrap().await},if sender.is_some()=>{
                    sender=None;return worker_finished(&mut state,"sender",result.map_err(|e|e.to_string()).and_then(|result|result.map(|_|()))).await.map_err(Into::into);
                },
                result=async{receiver.as_mut().unwrap().await},if receiver.is_some()=>{
                    receiver=None;let result=result.map_err(|e|e.to_string()).and_then(|stats|if stats.track_errors>0{Err("remote track failed".into())}else{Ok(())});return worker_finished(&mut state,"receiver",result).await.map_err(Into::into);
                },
                result=async{player.as_mut().unwrap().await},if player.is_some()=>{
                    player=None;return worker_finished(&mut state,"playback",result.map_err(|e|e.to_string()).and_then(|result|result.map(|_|()))).await.map_err(Into::into);
                },
                track=client.tracks.recv(),if receiver.is_none()=>{
                    let Some((remote,track))=track else{return worker_finished(&mut state,"remote track",Ok(())).await.map_err(Into::into);};
                    let (tx,rx)=mpsc::channel(4);
                    let worker=AudioReceiver::new(track,remote,tx,48000,2,20).map_err(std::io::Error::other)?;
                    let playback=PlaybackWorker::new(output.take().ok_or("duplicate track")?,rx);
                    receiver=Some(tokio::task::spawn_local(worker.run_until(stopped.clone())));
                    player=Some(tokio::task::spawn_local(playback.run_until(stopped.clone())));
                },
            }
        }
    }.await;
    let _ = stop.send(true);
    console.request_shutdown();
    if let Some(manager) = &devices {
        manager.request_shutdown();
    }
    let mut errors = Vec::new();
    if let Some(task) = sender {
        match task.await {
            Ok(Ok(stats)) => println!("Sender: {stats:?}"),
            other => errors.push(format!("sender cleanup: {other:?}")),
        }
    }
    if let Some(task) = receiver {
        match task.await {
            Ok(stats) => println!("Receiver: {stats:?}"),
            Err(error) => errors.push(format!("receiver cleanup: {error}")),
        }
    }
    if let Some(task) = player {
        match task.await {
            Ok(Ok(stats)) => println!("Playback: {stats:?}"),
            other => errors.push(format!("playback cleanup: {other:?}")),
        }
    }
    // Close the call before waiting for a native open operation to return.
    let closed = client.close().await;
    if let Some(devices) = devices {
        if let Err(error) = devices.shutdown().await {
            errors.push(error);
        }
    }
    if let Err(error) = console.shutdown().await {
        errors.push(error);
    }
    outcome?;
    closed?;
    if !errors.is_empty() {
        return Err(errors.join("; ").into());
    }
    Ok(())
}
type DeviceAction =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<DeviceResult, String>>>>;
enum DeviceResult {
    Listed(
        Vec<crate::device_audio::Entry>,
        Vec<crate::device_audio::Entry>,
    ),
    Changed(
        crate::device_audio::Direction,
        crate::device_audio::DeviceId,
    ),
}

/// A track may end just before the ordered signaling Leave reaches the client.
/// Give that notification a bounded grace, rather than misclassifying normal hangup.
async fn worker_finished(
    state: &mut watch::Receiver<SessionState>,
    role: &str,
    result: Result<(), String>,
) -> Result<(), String> {
    if let Err(error) = result {
        return Err(format!("audio {role} failed: {error}"));
    }
    let mut deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut closing = false;
    loop {
        match state.borrow_and_update().clone() {
            SessionState::Closed => return Ok(()),
            SessionState::Failed(error) => return Err(error),
            SessionState::Closing if !closing => {
                // Signaling writes can take up to two seconds during teardown.
                deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                closing = true;
            }
            _ => (),
        }
        match tokio::time::timeout_at(deadline, state.changed()).await {
            Ok(Ok(())) => (),
            Ok(Err(_)) => return Err("session state closed".into()),
            Err(_) => {
                return Err(format!(
                    "audio {role} stopped unexpectedly while call is active"
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn normal_remote_hangup_after_worker_exit_is_success() {
        let (state, mut receiver) = watch::channel(SessionState::Connected);
        let close = tokio::spawn(async move {
            tokio::task::yield_now().await;
            state.send_replace(SessionState::Closing);
            tokio::task::yield_now().await;
            state.send_replace(SessionState::Closed);
        });
        assert!(
            worker_finished(&mut receiver, "receiver", Ok(()))
                .await
                .is_ok()
        );
        close.await.unwrap();
    }
    #[tokio::test]
    async fn each_unexpected_worker_completion_and_failure_is_reported() {
        for role in ["sender", "receiver", "playback"] {
            for result in [Ok(()), Err("injected backend failure".into())] {
                let (_state, mut receiver) = watch::channel(SessionState::Connected);
                let error = worker_finished(&mut receiver, role, result)
                    .await
                    .unwrap_err();
                assert!(error.contains(role));
            }
        }
    }
    #[tokio::test]
    async fn backend_failure_is_not_hidden_by_a_concurrent_normal_close() {
        let (_state, mut receiver) = watch::channel(SessionState::Closed);
        assert!(
            worker_finished(
                &mut receiver,
                "playback",
                Err("output device failed".into())
            )
            .await
            .unwrap_err()
            .contains("output device failed")
        );
    }
    #[tokio::test]
    async fn session_failure_is_not_misclassified_as_normal_worker_exit() {
        let (_state, mut receiver) = watch::channel(SessionState::Failed("ICE failed".into()));
        assert_eq!(
            worker_finished(&mut receiver, "receiver", Ok(()))
                .await
                .unwrap_err(),
            "ICE failed"
        );
    }
}

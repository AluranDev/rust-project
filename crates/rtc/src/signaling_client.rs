use crate::{
    negotiated_audio::NegotiatedAudio,
    signaling::{Envelope, IceCandidateData, SignalMessage, WireMessage},
    webrtc_connection::WebRtcConnection,
};
use futures_util::{SinkExt, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use webrtc::{media_stream::track_remote::TrackRemote, peer_connection::RTCPeerConnectionState};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    Waiting,
    Negotiating,
    Connected,
    Reconnecting,
    Closing,
    Closed,
    Failed(String),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionCommand {
    SetMuted(bool),
    Leave,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEvent {
    StateChanged(SessionState),
    AudioNegotiated(NegotiatedAudio),
    Muted(bool),
    IceBuffered,
    StaleSignalDiscarded,
    SignalingRestored,
}
/// Join starts the voice call automatically when the second participant arrives.
#[derive(Clone)]
pub struct JoinSession {
    pub url: String,
    pub room: String,
    pub local: String,
    pub ssrc: u32,
    pub token: String,
    pub negotiation_timeout: Duration,
    pub recovery_timeout: Duration,
    pub recovery_backoff: Duration,
}
impl JoinSession {
    pub fn local(url: &str, room: &str, local: &str, ssrc: u32) -> Self {
        Self {
            url: url.into(),
            room: room.into(),
            local: local.into(),
            ssrc,
            token: "local-dev-token".into(),
            negotiation_timeout: Duration::from_secs(15),
            recovery_timeout: Duration::from_secs(6),
            recovery_backoff: Duration::from_millis(100),
        }
    }
}
struct CommandRequest {
    command: SessionCommand,
    reply: oneshot::Sender<()>,
}
pub struct SignalingClient {
    pub state: watch::Receiver<SessionState>,
    pub events: broadcast::Receiver<SessionEvent>,
    pub peer: Arc<WebRtcConnection>,
    pub tracks: mpsc::UnboundedReceiver<(String, Arc<dyn TrackRemote>)>,
    commands: mpsc::Sender<CommandRequest>,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), String>>,
}
impl SignalingClient {
    pub async fn connect(url: &str, room: &str, local: &str, ssrc: u32) -> Result<Self, Error> {
        Self::join(JoinSession::local(url, room, local, ssrc)).await
    }
    pub async fn join(config: JoinSession) -> Result<Self, Error> {
        let recovery_config = config.clone();
        let mut resume_ticket = None;
        let (events_tx, events) = broadcast::channel(64);
        let (state_tx, state) = watch::channel(SessionState::Waiting);
        publish(&state_tx, &events_tx, SessionState::Waiting);
        let (mut ws, _) =
            tokio::time::timeout(Duration::from_secs(10), connect_async(&config.url)).await??;
        send(
            &mut ws,
            0,
            WireMessage::JoinResumable {
                room: config.room,
                peer: config.local.clone(),
                token: config.token,
            },
        )
        .await?;
        let (generation, remote, initiator) =
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    match ws.next().await.ok_or("signaling closed before ready")?? {
                        Message::Text(text) => {
                            let env: Envelope = serde_json::from_str(&text)?;
                            if env.version != 1 {
                                return Err::<_, Error>("unsupported protocol".into());
                            }
                            match env.body {
                                WireMessage::Ready { remote, initiator } if env.generation != 0 => {
                                    return Ok((env.generation, remote, initiator));
                                }
                                WireMessage::Joined { ticket } => resume_ticket = Some(ticket),
                                WireMessage::Waiting => (),
                                WireMessage::Error { message } => return Err(message.into()),
                                _ => return Err("unexpected signaling message before ready".into()),
                            }
                        }
                        Message::Ping(data) => write(&mut ws, Message::Pong(data)).await?,
                        Message::Close(_) => return Err("signaling closed before ready".into()),
                        _ => (),
                    }
                }
            })
            .await??;
        // This deadline includes PeerConnection construction and offer creation.
        let deadline = tokio::time::Instant::now() + config.negotiation_timeout;
        let (signal_tx, mut signal_rx) = mpsc::unbounded_channel();
        let (track_tx, tracks) = mpsc::unbounded_channel();
        let peer = Arc::new(
            tokio::time::timeout_at(
                deadline,
                WebRtcConnection::new(config.ssrc, config.local, remote, signal_tx, track_tx),
            )
            .await??,
        );
        let (stop, mut stopped) = oneshot::channel();
        let (commands, mut command_rx) = mpsc::channel::<CommandRequest>(8);
        let task_peer = peer.clone();
        let mut connection_state = peer.subscribe_state();
        publish(&state_tx, &events_tx, SessionState::Negotiating);
        let task = tokio::spawn(async move {
            let mut ws = Some(ws);
            let mut recovery: Option<Recovery> = None;
            let mut outgoing = std::collections::VecDeque::new();
            let run = async {
                let mut connected = false;
                let mut remote_set = false;
                let mut pending: Vec<IceCandidateData> = Vec::new();
                if initiator {
                    let sdp = tokio::time::timeout_at(deadline, task_peer.create_offer()).await??;
                    send(ws.as_mut().unwrap(), generation, WireMessage::Offer { sdp }).await?;
                }
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline), if !connected => return Err::<(), Error>("negotiation timed out".into()),
                        _ = &mut stopped => break,
                        command = command_rx.recv() => {
                            let Some(request) = command else { break; };
                            match request.command {
                                SessionCommand::SetMuted(muted) => {
                                    task_peer.set_muted(muted);
                                    let _ = events_tx.send(SessionEvent::Muted(muted));
                                    let _ = request.reply.send(());
                                },
                                SessionCommand::Leave => {
                                    let _ = request.reply.send(());
                                    break;
                                }
                            }
                        },
                        changed = connection_state.changed() => {
                            changed.map_err(|_| "connection state closed")?;
                            let state = *connection_state.borrow_and_update();
                            match state {
                                RTCPeerConnectionState::Connected => {
                                    if !connected { tokio::time::timeout_at(deadline,task_peer.verify_negotiated_audio()).await??; }
                                    connected = true;
                                    if recovery.is_none() { publish(&state_tx, &events_tx, SessionState::Connected); }
                                },
                                RTCPeerConnectionState::Failed => return Err("peer connection failed".into()),
                                RTCPeerConnectionState::Closed => break,
                                _ => (),
                            }
                        },
                        event = signal_rx.recv() => match event {
                            Some(SignalMessage::IceCandidate { candidate, .. }) => {
                                if outgoing.len() >= 128 { return Err("too many outgoing ICE candidates".into()); }
                                outgoing.push_back(WireMessage::Ice { candidate });
                                if let Some(socket) = ws.as_mut() {
                                    while let Some(message) = outgoing.front().cloned() {
                                        if let Err(error) = send(socket,generation,message).await {
                                            if !connected { return Err(error); }
                                            ws = None;
                                            recovery = Some(recover(&recovery_config,generation,resume_ticket.clone().ok_or("server does not support resume")?));
                                            publish(&state_tx,&events_tx,SessionState::Reconnecting);
                                            break;
                                        }
                                        outgoing.pop_front();
                                    }
                                }
                            },
                            None => return Err("ICE event channel closed".into()),
                            _ => (),
                        },
                        restored = async { recovery.as_mut().unwrap().await }, if recovery.is_some() => {
                            let (socket,ticket) = restored?;
                            resume_ticket = Some(ticket);
                            ws = Some(socket);
                            recovery = None;
                            while let Some(message) = outgoing.front().cloned() {
                                if send(ws.as_mut().unwrap(),generation,message).await.is_err() {
                                    ws = None;
                                    recovery = Some(recover(&recovery_config,generation,resume_ticket.clone().unwrap()));
                                    break;
                                }
                                outgoing.pop_front();
                            }
                            if recovery.is_none() {
                                let _ = events_tx.send(SessionEvent::SignalingRestored);
                                publish(&state_tx,&events_tx,SessionState::Connected);
                            }
                        },
                        message = async { ws.as_mut().unwrap().next().await }, if ws.is_some() => match message {
                            Some(Ok(Message::Text(text))) => {
                                let env: Envelope = serde_json::from_str(&text)?;
                                if env.version != 1 { return Err("unsupported protocol".into()); }
                                if env.generation != generation {
                                    let _ = events_tx.send(SessionEvent::StaleSignalDiscarded);
                                    continue;
                                }
                                match env.body {
                                    WireMessage::Offer { sdp } if !initiator && !remote_set => {
                                        let answer = tokio::time::timeout_at(deadline, task_peer.accept_offer(&sdp)).await??;
                                        remote_set = true;
                                        for candidate in pending.drain(..) { task_peer.add_ice_candidate(&candidate).await?; }
                                        send(ws.as_mut().unwrap(), generation, WireMessage::Answer { sdp: answer }).await?;
                                        let _ = events_tx.send(SessionEvent::AudioNegotiated(task_peer.negotiated_audio().ok_or("audio not negotiated")?));
                                    },
                                    WireMessage::Answer { sdp } if initiator && !remote_set => {
                                        tokio::time::timeout_at(deadline, task_peer.accept_answer(&sdp)).await??;
                                        remote_set = true;
                                        for candidate in pending.drain(..) { task_peer.add_ice_candidate(&candidate).await?; }
                                        let _ = events_tx.send(SessionEvent::AudioNegotiated(task_peer.negotiated_audio().ok_or("audio not negotiated")?));
                                    },
                                    WireMessage::Ice { candidate } => {
                                        if remote_set { task_peer.add_ice_candidate(&candidate).await?; }
                                        else if pending.len() < 128 {
                                            pending.push(candidate);
                                            let _ = events_tx.send(SessionEvent::IceBuffered);
                                        } else { return Err("too many pending ICE candidates".into()); }
                                    },
                                    WireMessage::Leave => break,
                                    WireMessage::Error { message } => return Err(message.into()),
                                    _ => return Err("unexpected signaling message".into()),
                                }
                            },
                            Some(Ok(Message::Ping(data))) => {
                                if let Err(error) = write(ws.as_mut().unwrap(),Message::Pong(data)).await {
                                    if !connected { return Err(error); }
                                    ws = None;
                                    recovery = Some(recover(&recovery_config,generation,resume_ticket.clone().ok_or("server does not support resume")?));
                                    publish(&state_tx,&events_tx,SessionState::Reconnecting);
                                }
                            },
                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                                if !connected { return Err("signaling disconnected during negotiation".into()); }
                                ws = None;
                                recovery = Some(recover(&recovery_config,generation,resume_ticket.clone().ok_or("server does not support resume")?));
                                publish(&state_tx,&events_tx,SessionState::Reconnecting);
                            },
                            _ => (),
                        }
                    }
                }
                Ok(())
            }.await;
            publish(&state_tx, &events_tx, SessionState::Closing);
            // Tell the partner first so normal media teardown is distinguishable
            // from an unexpected track failure. Offline memberships expire by grace.
            if let Some(socket) = ws.as_mut() {
                let _ = send(socket, generation, WireMessage::Leave).await;
            }
            let cleanup = task_peer.close().await;
            if let Some(socket) = ws.as_mut() {
                let _ = tokio::time::timeout(Duration::from_secs(2), socket.close(None)).await;
            }
            let result = run.and(cleanup).map_err(|error| error.to_string());
            publish(
                &state_tx,
                &events_tx,
                match &result {
                    Ok(()) => SessionState::Closed,
                    Err(error) => SessionState::Failed(error.clone()),
                },
            );
            result
        });
        Ok(Self {
            state,
            events,
            peer,
            tracks,
            commands,
            stop: Some(stop),
            task,
        })
    }
    pub async fn command(&self, command: SessionCommand) -> Result<(), Error> {
        let (reply, ack) = oneshot::channel();
        self.commands
            .send(CommandRequest { command, reply })
            .await
            .map_err(|_| "session closed")?;
        tokio::time::timeout(Duration::from_secs(3), ack).await??;
        Ok(())
    }
    /// Returns only after the signaling task and the PeerConnection have closed.
    pub async fn close(mut self) -> Result<(), Error> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task.await?.map_err(Into::into)
    }
}
fn publish(
    state: &watch::Sender<SessionState>,
    events: &broadcast::Sender<SessionEvent>,
    value: SessionState,
) {
    state.send_replace(value.clone());
    let _ = events.send(SessionEvent::StateChanged(value));
}
async fn write(ws: &mut Socket, message: Message) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(2), ws.send(message)).await??;
    Ok(())
}
async fn send(ws: &mut Socket, generation: u64, body: WireMessage) -> Result<(), Error> {
    write(
        ws,
        Message::Text(serde_json::to_string(&Envelope::new(generation, body))?.into()),
    )
    .await
}

type Recovery =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(Socket, String), Error>> + Send>>;
fn recover(config: &JoinSession, generation: u64, ticket: String) -> Recovery {
    let config = config.clone();
    Box::pin(async move {
        let attempt = async {
            let mut backoff = config.recovery_backoff.max(Duration::from_millis(10));
            loop {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
                let connection =
                    tokio::time::timeout(Duration::from_secs(2), connect_async(&config.url)).await;
                let Ok(Ok((mut socket, _))) = connection else {
                    continue;
                };
                if send(
                    &mut socket,
                    generation,
                    WireMessage::Resume {
                        room: config.room.clone(),
                        peer: config.local.clone(),
                        token: config.token.clone(),
                        ticket: ticket.clone(),
                    },
                )
                .await
                .is_err()
                {
                    continue;
                }
                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            let message: Envelope = serde_json::from_str(&text)?;
                            if message.version != 1 || message.generation != generation {
                                return Err::<_, Error>(
                                    "invalid resume generation or version".into(),
                                );
                            }
                            match message.body {
                                WireMessage::Resumed { ticket } => return Ok((socket, ticket)),
                                WireMessage::Error { message } => return Err(message.into()),
                                WireMessage::Leave => {
                                    return Err("session expired while resuming".into());
                                }
                                _ => return Err("invalid resume response".into()),
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            if write(&mut socket, Message::Pong(data)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => (),
                    }
                }
            }
        };
        tokio::time::timeout(config.recovery_timeout, attempt)
            .await
            .map_err(|_| -> Error { "signaling recovery timed out".into() })?
    })
}

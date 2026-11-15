use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use rtc::signaling::{Envelope, WireMessage};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, mpsc, watch};

#[derive(Clone)]
pub struct ServerState {
    inner: Arc<Mutex<Rooms>>,
    active: Arc<AtomicUsize>,
    heartbeat_timeout: Duration,
    resume_grace: Duration,
    token: Arc<str>,
    burst: u32,
    per_second: u32,
}
impl Default for ServerState {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            active: Default::default(),
            heartbeat_timeout: Duration::from_secs(60),
            resume_grace: Duration::from_secs(8),
            token: "local-dev-token".into(),
            burst: 128,
            per_second: 64,
        }
    }
}
#[derive(Default)]
struct Rooms {
    rooms: HashMap<String, Room>,
    next: u64,
}
struct Room {
    members: Vec<Member>,
    generation: u64,
}
struct Member {
    id: String,
    connection: u64,
    tx: Option<mpsc::Sender<Envelope>>,
    end: watch::Sender<Control>,
    ticket: Option<String>,
    pending: VecDeque<Envelope>,
}
#[derive(Clone)]
enum Control {
    Active,
    Finish(u64),
    Disconnect,
    Replaced,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl ServerState {
    pub fn with_heartbeat_timeout(timeout: Duration) -> Self {
        assert!(timeout >= Duration::from_millis(3));
        Self {
            heartbeat_timeout: timeout,
            ..Self::default()
        }
    }
    pub fn with_resume_grace(mut self, grace: Duration) -> Self {
        self.resume_grace = grace;
        self
    }
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = token.into();
        self
    }
    pub fn with_rate_limit(mut self, burst: u32, per_second: u32) -> Self {
        assert!(burst > 0 && per_second > 0);
        self.burst = burst;
        self.per_second = per_second;
        self
    }
    pub async fn room_count(&self) -> usize {
        self.inner.lock().await.rooms.len()
    }
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
    /// Administrative transport interruption. Membership is retained for its resume grace.
    pub async fn disconnect_signaling(&self, room: &str, peer: &str) -> bool {
        let rooms = self.inner.lock().await;
        if let Some(member) = rooms
            .rooms
            .get(room)
            .and_then(|r| r.members.iter().find(|m| m.id == peer))
        {
            member.end.send_replace(Control::Disconnect);
            true
        } else {
            false
        }
    }
}
struct RateLimit {
    available: f64,
    updated: tokio::time::Instant,
    burst: u32,
    per_second: u32,
}
impl RateLimit {
    fn new(state: &ServerState) -> Self {
        Self {
            available: state.burst as f64,
            updated: tokio::time::Instant::now(),
            burst: state.burst,
            per_second: state.per_second,
        }
    }
    fn allow(&mut self) -> bool {
        let now = tokio::time::Instant::now();
        self.available = (self.available
            + now.duration_since(self.updated).as_secs_f64() * self.per_second as f64)
            .min(self.burst as f64);
        self.updated = now;
        if self.available < 1.0 {
            false
        } else {
            self.available -= 1.0;
            true
        }
    }
}
fn ticket() -> Result<String, getrandom::Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
pub fn router(state: ServerState) -> Router {
    Router::new().route("/ws", get(upgrade)).with_state(state)
}
async fn upgrade(ws: WebSocketUpgrade, State(state): State<ServerState>) -> impl IntoResponse {
    ws.max_message_size(65536)
        .max_frame_size(65536)
        .on_upgrade(move |socket| handle(socket, state))
}
type Sink = futures_util::stream::SplitSink<WebSocket, Message>;
async fn write(sink: &mut Sink, message: Message) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(2), sink.send(message)).await,
        Ok(Ok(()))
    )
}
async fn deliver(sink: &mut Sink, message: Envelope) -> bool {
    write(
        sink,
        Message::Text(serde_json::to_string(&message).unwrap().into()),
    )
    .await
}
fn end_room(rooms: &mut Rooms, room: &str, connection: u64) {
    if rooms
        .rooms
        .get(room)
        .is_some_and(|r| r.members.iter().any(|m| m.connection == connection))
    {
        let old = rooms.rooms.remove(room).unwrap();
        for member in old.members {
            member.end.send_replace(Control::Finish(old.generation));
        }
    }
}
async fn handle(socket: WebSocket, state: ServerState) {
    state.active.fetch_add(1, Ordering::SeqCst);
    let _active = Active(state.active.clone());
    let (mut sink, mut stream) = socket.split();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next()).await;
    let Some(Ok(Message::Text(text))) = first.ok().flatten() else {
        return;
    };
    let Ok(env) = serde_json::from_str::<Envelope>(&text) else {
        return;
    };
    if env.version != 1 {
        return;
    }
    let (room, peer, token, resume_ticket, resumable) = match env.body {
        WireMessage::Join { room, peer, token } if env.generation == 0 => {
            (room, peer, token, None, false)
        }
        WireMessage::JoinResumable { room, peer, token } if env.generation == 0 => {
            (room, peer, token, None, true)
        }
        WireMessage::Resume {
            room,
            peer,
            token,
            ticket,
        } => (room, peer, token, Some(ticket), true),
        _ => return,
    };
    if token != state.token.as_ref()
        || room.is_empty()
        || room.len() > 128
        || peer.is_empty()
        || peer.len() > 128
    {
        return;
    }
    let new_ticket = if resumable {
        let Ok(value) = ticket() else {
            return;
        };
        Some(value)
    } else {
        None
    };
    let (tx, mut rx) = mpsc::channel::<Envelope>(256);
    let (end, mut ended) = watch::channel(Control::Active);
    let connection = {
        let mut rooms = state.inner.lock().await;
        if let Some(resume_ticket) = resume_ticket {
            let member = rooms
                .rooms
                .get_mut(&room)
                .filter(|r| r.generation == env.generation)
                .and_then(|r| {
                    r.members.iter_mut().find(|m| {
                        m.id == peer && m.ticket.as_deref() == Some(resume_ticket.as_str())
                    })
                });
            let Some(member) = member else {
                drop(rooms);
                deliver(
                    &mut sink,
                    Envelope::new(
                        env.generation,
                        WireMessage::Error {
                            message: "resume rejected: expired or invalid membership".into(),
                        },
                    ),
                )
                .await;
                return;
            };
            // A valid ticket can replace a half-open socket. Rotate it atomically so
            // replaying the old ticket cannot create another active connection.
            member.end.send_replace(Control::Replaced);
            member.end = end;
            member.tx = Some(tx.clone());
            member.ticket = new_ticket.clone();
            tx.try_send(Envelope::new(
                env.generation,
                WireMessage::Resumed {
                    ticket: new_ticket.clone().unwrap(),
                },
            ))
            .unwrap();
            for message in member.pending.drain(..) {
                tx.try_send(message).unwrap();
            }
            rooms.next += 1;
            let connection = rooms.next;
            rooms
                .rooms
                .get_mut(&room)
                .unwrap()
                .members
                .iter_mut()
                .find(|m| m.id == peer)
                .unwrap()
                .connection = connection;
            connection
        } else {
            if rooms
                .rooms
                .get(&room)
                .is_some_and(|r| r.members.len() >= 2 || r.members.iter().any(|m| m.id == peer))
            {
                drop(rooms);
                deliver(
                    &mut sink,
                    Envelope::new(
                        0,
                        WireMessage::Error {
                            message: "room full or duplicate participant".into(),
                        },
                    ),
                )
                .await;
                return;
            }
            rooms.next += 1;
            let connection = rooms.next;
            let entry = rooms.rooms.entry(room.clone()).or_insert_with(|| Room {
                members: Vec::new(),
                generation: 0,
            });
            entry.members.push(Member {
                id: peer.clone(),
                connection,
                tx: Some(tx.clone()),
                end,
                ticket: new_ticket.clone(),
                pending: VecDeque::new(),
            });
            if let Some(ticket) = new_ticket {
                tx.try_send(Envelope::new(0, WireMessage::Joined { ticket }))
                    .unwrap();
            }
            if entry.members.len() == 2 {
                entry.generation = connection;
                for index in 0..2 {
                    let message = Envelope::new(
                        connection,
                        WireMessage::Ready {
                            remote: entry.members[1 - index].id.clone(),
                            initiator: index == 0,
                        },
                    );
                    let member = &mut entry.members[index];
                    if let Some(tx) = &member.tx {
                        let _ = tx.try_send(message);
                    } else {
                        member.pending.push_back(message);
                    }
                }
            } else {
                tx.try_send(Envelope::new(0, WireMessage::Waiting)).unwrap();
            }
            connection
        }
    };
    let mut idle = tokio::time::interval(state.heartbeat_timeout / 3);
    let mut last_pong = tokio::time::Instant::now();
    let mut rate = RateLimit::new(&state);
    let mut recoverable = true;
    loop {
        tokio::select! {
            changed=ended.changed()=>{
                if changed.is_ok() {
                    let control=ended.borrow_and_update().clone();
                    match control {
                        Control::Finish(generation)=>{ deliver(&mut sink,Envelope::new(generation,WireMessage::Leave)).await; return; },
                        Control::Replaced=>return,
                        Control::Disconnect=>break,
                        Control::Active=>(),
                    }
                } else { return; }
            },
            msg=rx.recv()=>{
                let Some(msg)=msg else {break;};
                if !deliver(&mut sink,msg).await {break;}
            },
            incoming=stream.next()=>{
                let Some(Ok(message))=incoming else {break;};
                if !rate.allow() {recoverable=false;break;}
                match message {
                    Message::Text(text)=>{
                        let Ok(msg)=serde_json::from_str::<Envelope>(&text) else {recoverable=false;break;};
                        if msg.version!=1 {recoverable=false;break;}
                        let mut rooms=state.inner.lock().await;
                        let Some(entry)=rooms.rooms.get_mut(&room).filter(|r|r.members.iter().any(|m|m.connection==connection)) else {return;};
                        if msg.generation!=entry.generation {continue;}
                        if matches!(msg.body,WireMessage::Leave) {recoverable=false;break;}
                        if entry.generation==0 || !matches!(msg.body,WireMessage::Offer {..}|WireMessage::Answer {..}|WireMessage::Ice {..}) {recoverable=false;break;}
                        let Some(other)=entry.members.iter_mut().find(|m|m.connection!=connection) else {recoverable=false;break;};
                        if let Some(tx)=&other.tx {
                            if tx.try_send(msg).is_err() {recoverable=false;break;}
                        } else if other.pending.len()<128 {other.pending.push_back(msg);} else {recoverable=false;break;}
                    },
                    Message::Pong(_)=>last_pong=tokio::time::Instant::now(),
                    Message::Ping(_)=>(),
                    Message::Close(_)=>break,
                    Message::Binary(_)=>{recoverable=false;break;},
                }
            },
            _=idle.tick()=>{
                if last_pong.elapsed()>=state.heartbeat_timeout {break;}
                if !write(&mut sink,Message::Ping(Default::default())).await {break;}
            }
        }
    }
    // Drop the broken transport before waiting for resume, so the client detects EOF.
    drop(sink);
    drop(stream);
    let retained = {
        let mut rooms = state.inner.lock().await;
        let member = rooms
            .rooms
            .get_mut(&room)
            .and_then(|r| r.members.iter_mut().find(|m| m.connection == connection));
        match member {
            Some(member) if recoverable && member.ticket.is_some() => {
                member.tx = None;
                true
            }
            Some(_) => {
                end_room(&mut rooms, &room, connection);
                false
            }
            None => false,
        }
    };
    if retained {
        // The detached handler owns the expiry wait; resume/leave wakes it, so no
        // detached timer task survives a successful recovery or a normal close.
        tokio::select! { _=tokio::time::sleep(state.resume_grace)=>(), _=ended.changed()=>() }
        end_room(&mut *state.inner.lock().await, &room, connection);
    }
}

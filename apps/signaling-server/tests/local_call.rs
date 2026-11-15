use audio::codec::opus::OpusCodec;
use futures_util::{SinkExt, StreamExt};
use rtc::{
    audio_receiver::AudioReceiver,
    negotiated_audio::NegotiatedAudio,
    signaling::{Envelope, SignalMessage, WireMessage},
    signaling_client::{JoinSession, SessionCommand, SessionEvent, SessionState, SignalingClient},
    webrtc_connection::WebRtcConnection,
};
use signaling_server::{ServerState, router};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};
type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Server {
    url: String,
    state: ServerState,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(state: ServerState) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let app = router(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            url,
            state,
            stop,
            task,
        }
    }
    async fn idle(&self) {
        tokio::time::timeout(Duration::from_secs(4), async {
            while self.state.active_connections() != 0 || self.state.room_count().await != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("server leaked rooms or websocket handlers");
    }
    async fn finish(self) {
        self.idle().await;
        self.stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
async fn send(socket: &mut Socket, generation: u64, body: WireMessage) {
    socket
        .send(Message::Text(
            serde_json::to_string(&Envelope::new(generation, body))
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
}
async fn next(socket: &mut Socket) -> Envelope {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await.expect("socket ended").unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                other => panic!("unexpected websocket message: {other:?}"),
            }
        }
    })
    .await
    .expect("no signaling response")
}
async fn join(url: &str, room: &str, peer: &str) -> Socket {
    let (mut socket, _) = connect_async(url).await.unwrap();
    send(
        &mut socket,
        0,
        WireMessage::Join {
            room: room.into(),
            peer: peer.into(),
            token: "local-dev-token".into(),
        },
    )
    .await;
    socket
}
async fn pair(url: &str) -> (Socket, Socket, u64) {
    let mut a = join(url, "room", "alice").await;
    assert!(matches!(next(&mut a).await.body, WireMessage::Waiting));
    let mut b = join(url, "room", "bob").await;
    let ready = next(&mut a).await;
    assert!(matches!(
        ready.body,
        WireMessage::Ready {
            initiator: true,
            ..
        }
    ));
    assert!(matches!(
        next(&mut b).await.body,
        WireMessage::Ready {
            initiator: false,
            ..
        }
    ));
    (a, b, ready.generation)
}
async fn connected(client: &SignalingClient) {
    let mut state = client.state.clone();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match state.borrow_and_update().clone() {
                SessionState::Connected => break,
                SessionState::Failed(error) => panic!("connection failed: {error}"),
                _ => (),
            }
            state.changed().await.unwrap();
        }
    })
    .await
    .expect("peer did not connect");
}
async fn terminal(client: &SignalingClient) -> SessionState {
    let mut state = client.state.clone();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let current = state.borrow_and_update().clone();
            if matches!(current, SessionState::Closed | SessionState::Failed(_)) {
                return current;
            }
            state.changed().await.unwrap();
        }
    })
    .await
    .expect("client never finished cleanup")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidirectional_audio_and_room_lifecycle() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let server = Server::start(ServerState::default()).await;
        let (a, b) = tokio::join!(SignalingClient::connect(&server.url, "room", "alice", 1234), SignalingClient::connect(&server.url, "room", "bob", 5678));
        let (mut a, mut b) = (a.unwrap(), b.unwrap());
        tokio::join!(connected(&a), connected(&b));
        assert_eq!(a.peer.negotiated_audio(), Some(NegotiatedAudio::OPUS));
        assert_eq!(b.peer.negotiated_audio(), Some(NegotiatedAudio::OPUS));
        let Err(error) = SignalingClient::connect(&server.url, "room", "third", 9012).await else { panic!("third participant admitted") };
        assert!(error.to_string().contains("room full"));
        a.command(SessionCommand::SetMuted(true)).await.unwrap();
        let mut muted = false; let mut negotiated = false; let mut waiting = false;
        while let Ok(event) = a.events.try_recv() {
            match event {
                SessionEvent::Muted(true) => muted = true,
                SessionEvent::AudioNegotiated(audio) => { assert_eq!(audio, NegotiatedAudio::OPUS); negotiated = true; },
                SessionEvent::StateChanged(SessionState::Waiting) => waiting = true,
                _ => (),
            }
        }
        assert!(muted && negotiated && waiting);
        a.command(SessionCommand::SetMuted(false)).await.unwrap();
        let (stop, mut stopped) = watch::channel(false);
        let pa = a.peer.clone(); let pb = b.peer.clone();
        let weak_a = Arc::downgrade(&pa); let weak_b = Arc::downgrade(&pb);
        let sender = tokio::spawn(async move {
            let mut ca = OpusCodec::new(48000, 2, 20).unwrap();
            let mut cb = OpusCodec::new(48000, 2, 20).unwrap();
            let mut packet_a = vec![0; 4000]; let mut packet_b = vec![0; 4000];
            let mut index = 0u64;
            loop {
                let pcm: Vec<f32> = (0..960).flat_map(|i| {
                    let s = (std::f32::consts::TAU * 440.0 * (index + i) as f32 / 48000.0).sin() * 0.2; [s, s]
                }).collect();
                index += 960;
                let na = ca.encode_frame(&pcm, &mut packet_a).unwrap();
                let nb = cb.encode_frame(&pcm, &mut packet_b).unwrap();
                pa.send_opus(&packet_a[..na], Duration::from_millis(20)).await.unwrap();
                pb.send_opus(&packet_b[..nb], Duration::from_millis(20)).await.unwrap();
                tokio::select! { _ = stopped.changed() => break, _ = tokio::time::sleep(Duration::from_millis(20)) => () }
            }
        });
        let (ta, tb) = tokio::join!(a.tracks.recv(), b.tracks.recv());
        let (ida, ta) = ta.unwrap(); let (idb, tb) = tb.unwrap();
        assert_eq!(ida, "bob"); assert_eq!(idb, "alice");
        let weak_track_a = Arc::downgrade(&ta); let weak_track_b = Arc::downgrade(&tb);
        let (txa, mut rxa) = mpsc::channel(8); let (txb, mut rxb) = mpsc::channel(8);
        let ra = tokio::spawn(AudioReceiver::new(ta, ida, txa, 48000, 2, 20).unwrap().run());
        let rb = tokio::spawn(AudioReceiver::new(tb, idb, txb, 48000, 2, 20).unwrap().run());
        for _ in 0..5 {
            let (fa, fb) = tokio::join!(rxa.recv(), rxb.recv());
            for frame in [fa.unwrap(), fb.unwrap()] {
                assert_eq!(frame.samples.len(), 1920);
                assert!(frame.samples.iter().all(|v| v.is_finite()));
                assert!(frame.samples.iter().any(|v| v.abs() > 0.00001));
            }
        }
        println!("both peers decoded five non-silent 1920-sample frames; codec and RTP payload validated");
        a.command(SessionCommand::SetMuted(true)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match tokio::time::timeout(Duration::from_millis(80), rxb.recv()).await {
                    Err(_) => break,
                    Ok(Some(_)) => (), // Drain any packet already in flight.
                    Ok(None) => panic!("muting ended the remote track"),
                }
            }
        }).await.expect("muted peer kept sending audio");
        a.command(SessionCommand::SetMuted(false)).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_secs(2), rxb.recv()).await.unwrap().is_some());
        stop.send(true).unwrap(); sender.await.unwrap();
        a.command(SessionCommand::Leave).await.unwrap();
        assert_eq!(terminal(&a).await, SessionState::Closed);
        assert_eq!(terminal(&b).await, SessionState::Closed);
        a.close().await.unwrap(); b.close().await.unwrap();
        for receiver in [ra, rb] {
            let stats = tokio::time::timeout(Duration::from_secs(3), receiver).await.expect("receiver did not end naturally").unwrap();
            assert!(stats.packets_decoded >= 5); assert_eq!(stats.incompatible_packets, 0); assert_eq!(stats.decode_errors, 0);
        }
        assert!(weak_a.upgrade().is_none() && weak_b.upgrade().is_none());
        assert!(weak_track_a.upgrade().is_none() && weak_track_b.upgrade().is_none());
        server.idle().await;
        for _ in 0..3 {
            let (a,b) = tokio::join!(SignalingClient::connect(&server.url,"room","alice",1234),SignalingClient::connect(&server.url,"room","bob",5678));
            let (a,b) = (a.unwrap(),b.unwrap());
            tokio::join!(connected(&a),connected(&b));
            a.close().await.unwrap(); b.close().await.unwrap();
            server.idle().await;
        }
        server.finish().await;
    }).await.expect("integration timed out");
}
#[tokio::test]
async fn heartbeat_reclaims_unresponsive_member() {
    let server = Server::start(ServerState::with_heartbeat_timeout(Duration::from_millis(
        150,
    )))
    .await;
    let mut stale = join(&server.url, "room", "alice").await;
    next(&mut stale).await;
    server.idle().await; // The socket stays alive without polling: no automatic pong.
    let mut replacement = join(&server.url, "room", "alice").await;
    assert!(matches!(
        next(&mut replacement).await.body,
        WireMessage::Waiting
    ));
    drop(replacement);
    drop(stale);
    server.finish().await;
}

#[tokio::test]
async fn negotiation_timeout_is_reported_and_closes_client() {
    let server = Server::start(ServerState::default()).await;
    let mut silent = join(&server.url, "room", "silent").await;
    next(&mut silent).await;
    let mut config = JoinSession::local(&server.url, "room", "bob", 5678);
    config.negotiation_timeout = Duration::from_millis(200);
    let client = SignalingClient::join(config).await.unwrap();
    let weak = Arc::downgrade(&client.peer);
    assert!(
        matches!(terminal(&client).await,SessionState::Failed(message) if message.contains("timed out"))
    );
    assert!(client.close().await.is_err());
    assert!(weak.upgrade().is_none());
    drop(silent);
    server.finish().await;
}

#[tokio::test]
async fn invalid_offer_reports_failure_and_closes_peer() {
    let server = Server::start(ServerState::default()).await;
    let mut remote = join(&server.url, "room", "alice").await;
    next(&mut remote).await;
    let client = SignalingClient::connect(&server.url, "room", "bob", 5678)
        .await
        .unwrap();
    let generation = next(&mut remote).await.generation;
    let weak = Arc::downgrade(&client.peer);
    let state = client.peer.subscribe_state();
    send(
        &mut remote,
        generation,
        WireMessage::Offer {
            sdp: "invalid".into(),
        },
    )
    .await;
    assert!(matches!(terminal(&client).await, SessionState::Failed(_)));
    assert_eq!(format!("{:?}", *state.borrow()), "Closed");
    assert!(client.close().await.is_err());
    assert!(weak.upgrade().is_none());
    drop(remote);
    server.finish().await;
}

#[tokio::test]
async fn abrupt_disconnect_and_rejoin_do_not_remove_new_members() {
    let server = Server::start(ServerState::default()).await;
    for _ in 0..8 {
        let (a, mut b, _) = pair(&server.url).await;
        drop(a); // TCP EOF, no Leave or WebSocket close handshake.
        assert!(matches!(next(&mut b).await.body, WireMessage::Leave));
        drop(b);
        // Deliberately do not wait for the old handler before reusing the room.
    }
    server.finish().await;
}

#[tokio::test]
async fn invalid_messages_authorization_and_room_isolation() {
    let server = Server::start(ServerState::default()).await;
    // Bad join token cannot reserve a place.
    let (mut bad, _) = connect_async(&server.url).await.unwrap();
    send(
        &mut bad,
        0,
        WireMessage::Join {
            room: "room".into(),
            peer: "alice".into(),
            token: "wrong".into(),
        },
    )
    .await;
    assert_disconnected(&mut bad).await;
    drop(bad);
    for malformed in [
        "not json",
        r#"{"version":2,"generation":0,"type":"join","room":"room","peer":"alice","token":"local-dev-token"}"#,
    ] {
        let (mut bad, _) = connect_async(&server.url).await.unwrap();
        bad.send(Message::Text(malformed.into())).await.unwrap();
        assert_disconnected(&mut bad).await;
        drop(bad);
    }
    let mut isolated = join(&server.url, "other-room", "alice").await;
    next(&mut isolated).await;
    let (mut a, mut b, generation) = pair(&server.url).await;
    let mut duplicate = join(&server.url, "room", "alice").await;
    assert!(matches!(
        next(&mut duplicate).await.body,
        WireMessage::Error { .. }
    ));
    drop(duplicate);
    // Old generation is discarded; marker from current generation must be first.
    send(
        &mut a,
        generation - 1,
        WireMessage::Offer {
            sdp: "stale".into(),
        },
    )
    .await;
    send(
        &mut a,
        generation,
        WireMessage::Offer {
            sdp: "current".into(),
        },
    )
    .await;
    assert!(matches!(next(&mut b).await.body,WireMessage::Offer { sdp } if sdp == "current"));
    a.send(Message::Text("invalid JSON".into())).await.unwrap();
    assert!(matches!(next(&mut b).await.body, WireMessage::Leave));
    drop(a);
    drop(b);
    // Other room survived the invalid sender.
    let mut partner = join(&server.url, "other-room", "bob").await;
    assert!(matches!(
        next(&mut isolated).await.body,
        WireMessage::Ready { .. }
    ));
    assert!(matches!(
        next(&mut partner).await.body,
        WireMessage::Ready { .. }
    ));
    drop(isolated);
    drop(partner);
    server.finish().await;
}
async fn assert_disconnected(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                Some(Ok(Message::Ping(data))) => {
                    let _ = socket.send(Message::Pong(data)).await;
                }
                _ => (),
            }
        }
    })
    .await
    .expect("invalid client was not disconnected");
}

#[tokio::test]
async fn rate_limit_disconnects_flooding_sender_and_cleans_room() {
    let server = Server::start(ServerState::default().with_rate_limit(4, 1)).await;
    let (mut a, mut b, generation) = pair(&server.url).await;
    for _ in 0..12 {
        let message = serde_json::to_string(&Envelope::new(
            generation,
            WireMessage::Offer {
                sdp: "flood".into(),
            },
        ))
        .unwrap();
        if a.send(Message::Text(message.into())).await.is_err() {
            break;
        }
    }
    loop {
        if matches!(next(&mut b).await.body, WireMessage::Leave) {
            break;
        }
    }
    assert_disconnected(&mut a).await;
    drop(a);
    drop(b);
    server.finish().await;
}

#[tokio::test]
async fn ice_before_sdp_is_buffered_and_applied() {
    let server = Server::start(ServerState::default()).await;
    let mut remote = join(&server.url, "room", "alice").await;
    next(&mut remote).await;
    let mut client = SignalingClient::connect(&server.url, "room", "bob", 5678)
        .await
        .unwrap();
    let generation = next(&mut remote).await.generation;
    let (signals, mut candidates) = mpsc::unbounded_channel();
    let (tracks, _tracks) = mpsc::unbounded_channel();
    let peer = WebRtcConnection::new(1234, "alice", "bob", signals, tracks)
        .await
        .unwrap();
    let offer = peer.create_offer().await.unwrap();
    let candidate = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(SignalMessage::IceCandidate { candidate, .. }) = candidates.recv().await {
                return candidate;
            }
        }
    })
    .await
    .unwrap();
    send(&mut remote, generation, WireMessage::Ice { candidate }).await;
    send(
        &mut remote,
        generation,
        WireMessage::Ice {
            candidate: rtc::signaling::IceCandidateData {
                candidate: String::new(),
                sdp_mid: None,
                sdp_mline_index: None,
                username_fragment: None,
                url: None,
            },
        },
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if client.events.recv().await.unwrap() == SessionEvent::IceBuffered {
                break;
            }
        }
    })
    .await
    .unwrap();
    send(&mut remote, generation, WireMessage::Offer { sdp: offer }).await;
    loop {
        match next(&mut remote).await.body {
            WireMessage::Answer { sdp } => {
                peer.accept_answer(&sdp).await.unwrap();
                break;
            }
            WireMessage::Ice { candidate } => peer.add_ice_candidate(&candidate).await.unwrap(),
            other => panic!("unexpected: {other:?}"),
        }
    }
    connected(&client).await;
    client.close().await.unwrap();
    peer.close().await.unwrap();
    drop(remote);
    server.finish().await;
}

#[tokio::test]
async fn client_discards_stale_generation_without_touching_peer() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let mock = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        assert!(matches!(ws.next().await, Some(Ok(Message::Text(_)))));
        for message in [
            Envelope::new(
                10,
                WireMessage::Ready {
                    remote: "alice".into(),
                    initiator: false,
                },
            ),
            Envelope::new(
                9,
                WireMessage::Offer {
                    sdp: "invalid stale SDP must not be parsed".into(),
                },
            ),
            Envelope::new(
                9,
                WireMessage::Ice {
                    candidate: rtc::signaling::IceCandidateData {
                        candidate: "invalid stale ICE must not be applied".into(),
                        sdp_mid: None,
                        sdp_mline_index: None,
                        username_fragment: None,
                        url: None,
                    },
                },
            ),
        ] {
            ws.send(Message::Text(
                serde_json::to_string(&message).unwrap().into(),
            ))
            .await
            .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(Ok(Message::Text(text))) = ws.next().await {
                let env: Envelope = serde_json::from_str(&text).unwrap();
                if matches!(env.body, WireMessage::Leave) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let _ = ws.close(None).await;
    });
    let mut client = SignalingClient::connect(&url, "room", "bob", 5678)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut count = 0;
        while count < 2 {
            if client.events.recv().await.unwrap() == SessionEvent::StaleSignalDiscarded {
                count += 1;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(*client.state.borrow(), SessionState::Negotiating);
    assert!(client.peer.negotiated_audio().is_none());
    client.command(SessionCommand::Leave).await.unwrap();
    client.close().await.unwrap();
    mock.await.unwrap();
}

#[tokio::test]
async fn dropping_a_client_closes_its_task_and_the_remote_session() {
    let server = Server::start(ServerState::default()).await;
    let (a, b) = tokio::join!(
        SignalingClient::connect(&server.url, "room", "alice", 1234),
        SignalingClient::connect(&server.url, "room", "bob", 5678)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    tokio::join!(connected(&a), connected(&b));
    let weak = Arc::downgrade(&a.peer);
    drop(a); // Dropping the stop sender must wake and clean up the detached task.
    assert_eq!(terminal(&b).await, SessionState::Closed);
    b.close().await.unwrap();
    server.idle().await;
    assert!(weak.upgrade().is_none());
    server.finish().await;
}

#[tokio::test]
async fn configured_local_credential_is_required() {
    let server = Server::start(ServerState::default().with_token("test-specific-token")).await;
    assert!(
        SignalingClient::connect(&server.url, "room", "alice", 1234)
            .await
            .is_err()
    );
    let mut ca = JoinSession::local(&server.url, "room", "alice", 1234);
    let mut cb = JoinSession::local(&server.url, "room", "bob", 5678);
    ca.token = "test-specific-token".into();
    cb.token = ca.token.clone();
    let (a, b) = tokio::join!(SignalingClient::join(ca), SignalingClient::join(cb));
    let (a, b) = (a.unwrap(), b.unwrap());
    tokio::join!(connected(&a), connected(&b));
    a.close().await.unwrap();
    b.close().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn signaling_can_arrive_before_other_member_reads_ready() {
    let server = Server::start(ServerState::default()).await;
    let mut a = join(&server.url, "room", "alice").await;
    next(&mut a).await;
    let mut b = join(&server.url, "room", "bob").await;
    let generation = next(&mut b).await.generation;
    send(
        &mut b,
        generation,
        WireMessage::Answer {
            sdp: "early marker".into(),
        },
    )
    .await;
    assert!(matches!(next(&mut a).await.body, WireMessage::Ready { .. }));
    assert!(matches!(next(&mut a).await.body,WireMessage::Answer { sdp } if sdp == "early marker"));
    drop(a);
    drop(b);
    server.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signaling_resume_preserves_live_bidirectional_media_and_rejects_duplicates() {
    let server = Server::start(ServerState::default()).await;
    let mut config = JoinSession::local(&server.url, "room", "alice", 1234);
    config.recovery_backoff = Duration::from_millis(300);
    let (a, b) = tokio::join!(
        SignalingClient::join(config),
        SignalingClient::connect(&server.url, "room", "bob", 5678)
    );
    let (mut a, mut b) = (a.unwrap(), b.unwrap());
    tokio::join!(connected(&a), connected(&b));
    let pa = a.peer.clone();
    let pb = b.peer.clone();
    let original = Arc::as_ptr(&a.peer);
    let (stop, mut stopped) = watch::channel(false);
    let sender = tokio::spawn(async move {
        let mut codec = OpusCodec::new(48000, 2, 20).unwrap();
        let pcm: Vec<f32> = (0..960)
            .flat_map(|i| {
                let value = (i as f32 * 0.0576).sin() * 0.2;
                [value, value]
            })
            .collect();
        let mut packet = vec![0; 4000];
        let n = codec.encode_frame(&pcm, &mut packet).unwrap();
        loop {
            pa.send_opus(&packet[..n], Duration::from_millis(20))
                .await
                .unwrap();
            pb.send_opus(&packet[..n], Duration::from_millis(20))
                .await
                .unwrap();
            tokio::select! {_=stopped.changed()=>break,_=tokio::time::sleep(Duration::from_millis(20))=>()}
        }
    });
    let (ta, tb) = tokio::join!(a.tracks.recv(), b.tracks.recv());
    let (ida, ta) = ta.unwrap();
    let (idb, tb) = tb.unwrap();
    let (txa, mut rxa) = mpsc::channel(32);
    let (txb, mut rxb) = mpsc::channel(32);
    let ra = tokio::spawn(
        AudioReceiver::new(ta, ida, txa, 48000, 2, 20)
            .unwrap()
            .run(),
    );
    let rb = tokio::spawn(
        AudioReceiver::new(tb, idb, txb, 48000, 2, 20)
            .unwrap()
            .run(),
    );
    rxa.recv().await.unwrap();
    rxb.recv().await.unwrap();
    let mut state = a.state.clone();
    assert!(server.state.disconnect_signaling("room", "alice").await);
    tokio::time::timeout(
        Duration::from_secs(2),
        state.wait_for(|s| matches!(s, SessionState::Reconnecting)),
    )
    .await
    .unwrap()
    .unwrap();
    a.command(SessionCommand::SetMuted(false)).await.unwrap();
    // Decode both directions while the signaling transport is down.
    for _ in 0..3 {
        let (fa, fb) = tokio::join!(rxa.recv(), rxb.recv());
        assert_eq!(fa.unwrap().samples.len(), 1920);
        assert_eq!(fb.unwrap().samples.len(), 1920);
        assert_eq!(*a.state.borrow(), SessionState::Reconnecting);
    }
    assert!(
        SignalingClient::connect(&server.url, "room", "alice", 42)
            .await
            .is_err()
    );
    assert!(
        SignalingClient::connect(&server.url, "room", "third", 43)
            .await
            .is_err()
    );
    tokio::time::timeout(
        Duration::from_secs(3),
        state.wait_for(|s| matches!(s, SessionState::Connected)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(Arc::as_ptr(&a.peer), original);
    let mut restored = false;
    while let Ok(event) = a.events.try_recv() {
        if event == SessionEvent::SignalingRestored {
            restored = true;
        }
    }
    assert!(restored);
    assert_eq!(server.state.room_count().await, 1);
    // Repeat with the rotated ticket to demonstrate that resume does not duplicate slots.
    assert!(server.state.disconnect_signaling("room", "alice").await);
    tokio::time::timeout(
        Duration::from_secs(2),
        state.wait_for(|s| matches!(s, SessionState::Reconnecting)),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        state.wait_for(|s| matches!(s, SessionState::Connected)),
    )
    .await
    .unwrap()
    .unwrap();
    stop.send(true).unwrap();
    sender.await.unwrap();
    a.close().await.unwrap();
    b.close().await.unwrap();
    for task in [ra, rb] {
        let stats = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(stats.packets_decoded >= 4);
        assert_eq!(stats.decode_errors, 0);
    }
    server.finish().await;
}

#[tokio::test]
async fn recovery_deadline_closes_media_and_server_grace_expires() {
    let Server {
        url,
        state,
        stop,
        task,
    } = Server::start(ServerState::default().with_resume_grace(Duration::from_millis(900))).await;
    let mut config = JoinSession::local(&url, "room", "alice", 1234);
    config.recovery_timeout = Duration::from_millis(300);
    config.recovery_backoff = Duration::from_millis(20);
    let (a, b) = tokio::join!(
        SignalingClient::join(config),
        SignalingClient::connect(&url, "room", "bob", 5678)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    tokio::join!(connected(&a), connected(&b));
    let weak = Arc::downgrade(&a.peer);
    stop.send(()).unwrap(); // Stop accepting new sockets; established WebSockets survive.
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(state.disconnect_signaling("room", "alice").await);
    assert!(
        matches!(terminal(&a).await,SessionState::Failed(error) if error.contains("recovery timed out"))
    );
    assert!(a.close().await.is_err());
    assert!(weak.upgrade().is_none());
    assert_eq!(terminal(&b).await, SessionState::Closed);
    b.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.room_count().await != 0 || state.active_connections() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn resumable_join(url: &str, peer: &str) -> (Socket, String) {
    let (mut ws, _) = connect_async(url).await.unwrap();
    send(
        &mut ws,
        0,
        WireMessage::JoinResumable {
            room: "room".into(),
            peer: peer.into(),
            token: "local-dev-token".into(),
        },
    )
    .await;
    let WireMessage::Joined { ticket } = next(&mut ws).await.body else {
        panic!("no ticket");
    };
    (ws, ticket)
}
#[tokio::test]
async fn resume_ticket_is_bound_to_room_peer_and_generation_and_rotates() {
    let server = Server::start(ServerState::default()).await;
    let (mut alice, ticket) = resumable_join(&server.url, "alice").await;
    next(&mut alice).await;
    let mut bob = join(&server.url, "room", "bob").await;
    let generation = next(&mut alice).await.generation;
    next(&mut bob).await;
    for (room, peer, old_generation, credential) in [
        ("other", "alice", generation, ticket.clone()),
        ("room", "bob", generation, ticket.clone()),
        ("room", "alice", generation + 1, ticket.clone()),
        ("room", "alice", generation, "wrong-ticket".into()),
    ] {
        let (mut ws, _) = connect_async(&server.url).await.unwrap();
        send(
            &mut ws,
            old_generation,
            WireMessage::Resume {
                room: room.into(),
                peer: peer.into(),
                token: "local-dev-token".into(),
                ticket: credential,
            },
        )
        .await;
        assert!(matches!(
            next(&mut ws).await.body,
            WireMessage::Error { .. }
        ));
    }
    // Authorize replacement of a half-open socket; membership and generation persist.
    let (mut resumed, _) = connect_async(&server.url).await.unwrap();
    send(
        &mut resumed,
        generation,
        WireMessage::Resume {
            room: "room".into(),
            peer: "alice".into(),
            token: "local-dev-token".into(),
            ticket: ticket.clone(),
        },
    )
    .await;
    let ack = next(&mut resumed).await;
    assert_eq!(ack.generation, generation);
    let WireMessage::Resumed { ticket: new_ticket } = ack.body else {
        panic!("not resumed");
    };
    assert_ne!(ticket, new_ticket);
    let (mut replay, _) = connect_async(&server.url).await.unwrap();
    send(
        &mut replay,
        generation,
        WireMessage::Resume {
            room: "room".into(),
            peer: "alice".into(),
            token: "local-dev-token".into(),
            ticket,
        },
    )
    .await;
    assert!(matches!(
        next(&mut replay).await.body,
        WireMessage::Error { .. }
    ));
    drop(replay);
    send(
        &mut resumed,
        generation,
        WireMessage::Offer {
            sdp: "still routed to the same partner".into(),
        },
    )
    .await;
    assert!(matches!(
        next(&mut bob).await.body,
        WireMessage::Offer { .. }
    ));
    send(&mut resumed, generation, WireMessage::Leave).await;
    assert!(matches!(next(&mut bob).await.body, WireMessage::Leave));
    drop(alice);
    drop(resumed);
    drop(bob);
    server.finish().await;
}

#[tokio::test]
async fn closing_during_recovery_cancels_backoff_and_expires_membership() {
    let server =
        Server::start(ServerState::default().with_resume_grace(Duration::from_millis(200))).await;
    let mut config = JoinSession::local(&server.url, "room", "alice", 1234);
    config.recovery_backoff = Duration::from_secs(10);
    let (a, b) = tokio::join!(
        SignalingClient::join(config),
        SignalingClient::connect(&server.url, "room", "bob", 5678)
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    tokio::join!(connected(&a), connected(&b));
    let weak = Arc::downgrade(&a.peer);
    let mut state = a.state.clone();
    server.state.disconnect_signaling("room", "alice").await;
    tokio::time::timeout(
        Duration::from_secs(1),
        state.wait_for(|s| matches!(s, SessionState::Reconnecting)),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_millis(500), a.close())
        .await
        .expect("backoff blocked cancellation")
        .unwrap();
    assert!(weak.upgrade().is_none());
    assert_eq!(terminal(&b).await, SessionState::Closed);
    b.close().await.unwrap();
    server.finish().await;
}

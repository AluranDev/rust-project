use std::sync::Arc;

use tokio::sync::mpsc::UnboundedSender;

use webrtc::media_stream::track_remote::TrackRemote;
use webrtc::peer_connection::{
    PeerConnectionEventHandler, RTCIceConnectionState, RTCIceGatheringState,
    RTCPeerConnectionIceEvent, RTCPeerConnectionState,
};

use crate::signaling::{IceCandidateData, SignalMessage};

pub struct VoiceEventHandler {
    local_id: String,
    remote_id: String,
    signal_tx: UnboundedSender<SignalMessage>,
    track_tx: UnboundedSender<(String, Arc<dyn TrackRemote>)>,
    pub state_tx: tokio::sync::watch::Sender<RTCPeerConnectionState>,
}

impl VoiceEventHandler {
    pub fn new(
        local_id: impl Into<String>,
        remote_id: impl Into<String>,
        signal_tx: UnboundedSender<SignalMessage>,
        track_tx: UnboundedSender<(String, Arc<dyn TrackRemote>)>,
    ) -> Self {
        Self {
            local_id: local_id.into(),
            remote_id: remote_id.into(),
            signal_tx,
            track_tx,
            state_tx: tokio::sync::watch::channel(RTCPeerConnectionState::New).0,
        }
    }
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for VoiceEventHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.signal_tx.send(SignalMessage::IceCandidate {
                from: self.local_id.clone(),
                candidate: IceCandidateData {
                    candidate: String::new(),
                    sdp_mid: None,
                    sdp_mline_index: None,
                    username_fragment: None,
                    url: None,
                },
            });
        }
    }

    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        let init = match event.candidate.to_json() {
            Ok(candidate) => candidate,
            Err(error) => {
                eprintln!("ICE candidate serialization error: {error}");
                return;
            }
        };

        let candidate = IceCandidateData {
            candidate: init.candidate,
            sdp_mid: init.sdp_mid,
            sdp_mline_index: init.sdp_mline_index,
            username_fragment: init.username_fragment,
            url: init.url,
        };

        let message = SignalMessage::IceCandidate {
            from: self.local_id.clone(),
            candidate,
        };

        if self.signal_tx.send(message).is_err() {
            eprintln!("Signaling receiver disconnected");
        }
    }

    async fn on_ice_connection_state_change(&self, state: RTCIceConnectionState) {
        println!("ICE state: {state:?}");
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        self.state_tx.send_replace(state);
        println!("Peer connection state: {state:?}");
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let ssrcs = track.ssrcs().await;

        println!(
            "Remote audio track received | Peer: {} | SSRCs: {:?}",
            self.remote_id, ssrcs
        );

        if self.track_tx.send((self.remote_id.clone(), track)).is_err() {
            eprintln!("Remote track receiver disconnected");
        }
    }
}

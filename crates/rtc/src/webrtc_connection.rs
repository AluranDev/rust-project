use std::sync::Arc;

use webrtc::peer_connection::{MediaEngine, PeerConnection, PeerConnectionBuilder};

use webrtc_core::peer_connection::sdp::RTCSessionDescription;

use crate::audio_track::{AudioTrack, register_opus_codec};

// Cancellation during construction must also release the newly built driver.
struct PendingPeer(Option<Arc<dyn PeerConnection>>);
impl Drop for PendingPeer {
    fn drop(&mut self) {
        if let Some(peer) = self.0.take() {
            tokio::spawn(async move {
                let _ = peer.close().await;
            });
        }
    }
}

pub struct WebRtcConnection {
    peer: Arc<dyn PeerConnection>,
    muted: std::sync::atomic::AtomicBool,
    negotiated: tokio::sync::watch::Sender<Option<crate::negotiated_audio::NegotiatedAudio>>,
    audio_track: AudioTrack,
    state: tokio::sync::watch::Receiver<webrtc::peer_connection::RTCPeerConnectionState>,
    state_tx: tokio::sync::watch::Sender<webrtc::peer_connection::RTCPeerConnectionState>,
}

impl WebRtcConnection {
    pub async fn add_ice_candidate(
        &self,
        candidate: &crate::signaling::IceCandidateData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let candidate = webrtc_core::peer_connection::transport::RTCIceCandidateInit {
            candidate: candidate.candidate.clone(),
            sdp_mid: candidate.sdp_mid.clone(),
            sdp_mline_index: candidate.sdp_mline_index,
            username_fragment: candidate.username_fragment.clone(),
            url: candidate.url.clone(),
        };

        self.peer.add_ice_candidate(candidate).await?;

        Ok(())
    }

    pub async fn new(
        ssrc: u32,
        local_id: impl Into<String>,
        remote_id: impl Into<String>,
        signal_tx: tokio::sync::mpsc::UnboundedSender<crate::signaling::SignalMessage>,
        track_tx: tokio::sync::mpsc::UnboundedSender<(
            String,
            Arc<dyn webrtc::media_stream::track_remote::TrackRemote>,
        )>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Configure media engine.
        let mut media_engine = MediaEngine::default();

        // M1 offers only the validated Opus profile.
        register_opus_codec(&mut media_engine)?;
        let audio_track = AudioTrack::new(ssrc)?;

        // Build PeerConnection.
        let handler = Arc::new(crate::event_handler::VoiceEventHandler::new(
            local_id, remote_id, signal_tx, track_tx,
        ));
        let state = handler.state_tx.subscribe();
        let state_tx = handler.state_tx.clone();

        let peer = PeerConnectionBuilder::new()
            .with_media_engine(media_engine)
            .with_handler(handler)
            .with_udp_addrs(vec!["0.0.0.0:0"])
            .build()
            .await?;

        let peer: Arc<dyn PeerConnection> = Arc::new(peer);
        let mut pending = PendingPeer(Some(peer.clone()));

        // Attach audio track to PeerConnection.
        if let Err(error) = audio_track.attach(&*peer).await {
            let _ = peer.close().await;
            pending.0.take();
            return Err(error);
        }
        pending.0.take();

        Ok(Self {
            peer,
            muted: std::sync::atomic::AtomicBool::new(false),
            negotiated: tokio::sync::watch::channel(None).0,
            audio_track,
            state,
            state_tx,
        })
    }

    pub fn subscribe_state(
        &self,
    ) -> tokio::sync::watch::Receiver<webrtc::peer_connection::RTCPeerConnectionState> {
        self.state.clone()
    }

    pub fn negotiated_audio(&self) -> Option<crate::negotiated_audio::NegotiatedAudio> {
        *self.negotiated.borrow()
    }

    /// Check the sender's actual negotiated parameters as well as the SDP profile.
    pub async fn verify_negotiated_audio(
        &self,
    ) -> Result<crate::negotiated_audio::NegotiatedAudio, Box<dyn std::error::Error + Send + Sync>>
    {
        let audio = self.negotiated_audio().ok_or("audio not negotiated")?;
        let senders = self.peer.get_senders().await;
        if senders.len() != 1 {
            return Err("M1 requires one audio sender".into());
        }
        let parameters = senders[0].get_parameters().await?;
        let codecs = parameters.rtp_parameters.codecs;
        if codecs.len() != 1
            || !codecs.iter().any(|codec| {
                codec.payload_type == audio.payload_type
                    && codec.rtp_codec.mime_type.eq_ignore_ascii_case("audio/opus")
                    && codec.rtp_codec.clock_rate == audio.sample_rate
                    && codec.rtp_codec.channels == audio.channels
            })
        {
            return Err("sender codec or payload type differs from negotiated Opus profile".into());
        }
        Ok(audio)
    }
    pub fn set_muted(&self, muted: bool) {
        self.muted
            .store(muted, std::sync::atomic::Ordering::Relaxed);
    }

    /// Creates an SDP offer.
    pub async fn create_offer(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let offer = self.peer.create_offer(None).await?;
        crate::negotiated_audio::validate_audio(&offer)?;

        self.peer.set_local_description(offer).await?;

        let local_description = self
            .peer
            .local_description()
            .await
            .ok_or("Local SDP description not available")?;

        Ok(local_description.sdp)
    }

    /// Receives a remote offer and creates an answer.
    pub async fn accept_offer(
        &self,
        remote_sdp: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let offer = RTCSessionDescription::offer(remote_sdp.to_owned())?;
        crate::negotiated_audio::validate_audio(&offer)?;

        self.peer.set_remote_description(offer).await?;

        let answer = self.peer.create_answer(None).await?;

        let audio = crate::negotiated_audio::validate_audio(&answer)?;
        self.peer.set_local_description(answer).await?;
        self.negotiated.send_replace(Some(audio));

        let local_description = self
            .peer
            .local_description()
            .await
            .ok_or("Local SDP description not available")?;

        Ok(local_description.sdp)
    }

    /// Applies the remote answer.
    pub async fn accept_answer(
        &self,
        remote_sdp: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let answer = RTCSessionDescription::answer(remote_sdp.to_owned())?;

        let audio = crate::negotiated_audio::validate_audio(&answer)?;
        self.peer.set_remote_description(answer).await?;
        self.negotiated.send_replace(Some(audio));

        Ok(())
    }

    /// Sends an encoded Opus frame.
    pub async fn send_opus(
        &self,
        encoded: &[u8],
        duration: std::time::Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *self.state.borrow() == webrtc::peer_connection::RTCPeerConnectionState::Closed {
            return Err("peer connection closed".into());
        }
        if self.negotiated.borrow().is_none() {
            return Err("audio not negotiated".into());
        }
        if !self.muted.load(std::sync::atomic::Ordering::Relaxed) {
            self.audio_track.write_opus(encoded, duration).await?;
        }

        Ok(())
    }

    /// Closes the connection.
    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.peer.close().await?;
        self.state_tx
            .send_replace(webrtc::peer_connection::RTCPeerConnectionState::Closed);

        Ok(())
    }
}

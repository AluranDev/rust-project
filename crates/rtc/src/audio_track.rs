use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use webrtc_core::media::Sample;
use webrtc_core::media_stream::MediaStreamTrack;
use webrtc_core::peer_connection::configuration::media_engine::MediaEngine;

use webrtc_core::rtp_transceiver::PayloadType;
use webrtc_core::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters,
    RtpCodecKind,
};

use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
use webrtc::peer_connection::PeerConnection;

const OPUS_PAYLOAD_TYPE: PayloadType = 111;
const OPUS_CLOCK_RATE: u32 = 48000;
const OPUS_CHANNELS: u16 = 2;

pub fn opus_codec_parameters() -> RTCRtpCodecParameters {
    RTCRtpCodecParameters {
        rtp_codec: RTCRtpCodec {
            mime_type: "audio/opus".into(),
            clock_rate: OPUS_CLOCK_RATE,
            channels: OPUS_CHANNELS,
            sdp_fmtp_line: "minptime=10;useinbandfec=1;stereo=1".into(),
            rtcp_feedback: vec![],
        },
        payload_type: OPUS_PAYLOAD_TYPE,
        ..Default::default()
    }
}

pub fn register_opus_codec(
    media_engine: &mut MediaEngine,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    media_engine.register_codec(opus_codec_parameters(), RtpCodecKind::Audio)?;

    Ok(())
}

pub struct AudioTrack {
    track: Arc<TrackLocalStaticSample>,
    ssrc: u32,
}

impl AudioTrack {
    pub fn new(ssrc: u32) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let codec = opus_codec_parameters();

        let media_track = MediaStreamTrack::new(
            "voice-app".into(),
            "microphone".into(),
            "Microphone".into(),
            RtpCodecKind::Audio,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                codec: codec.rtp_codec,
                ..Default::default()
            }],
        );

        let track = Arc::new(TrackLocalStaticSample::new(Instant::now(), media_track)?);

        Ok(Self { track, ssrc })
    }

    pub async fn attach(
        &self,
        peer_connection: &dyn PeerConnection,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        peer_connection
            .add_track(Arc::clone(&self.track) as Arc<dyn TrackLocal>)
            .await?;

        Ok(())
    }

    pub async fn write_opus(
        &self,
        encoded: &[u8],
        duration: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sample = Sample {
            data: Bytes::copy_from_slice(encoded),
            duration,
            ..Sample::new(Instant::now())
        };

        self.track
            .sample_writer(self.ssrc, OPUS_PAYLOAD_TYPE)
            .write_sample(&sample)
            .await?;

        Ok(())
    }

    pub fn track(&self) -> Arc<TrackLocalStaticSample> {
        Arc::clone(&self.track)
    }

    pub fn ssrc(&self) -> u32 {
        self.ssrc
    }
}

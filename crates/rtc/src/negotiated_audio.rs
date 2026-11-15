//! The M1 voice profile deliberately negotiates one codec: Opus/48000/2, PT 111.
use webrtc_core::peer_connection::sdp::RTCSessionDescription;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NegotiatedAudio {
    pub payload_type: u8,
    pub sample_rate: u32,
    pub channels: u16,
}
impl NegotiatedAudio {
    pub const OPUS: Self = Self {
        payload_type: 111,
        sample_rate: 48000,
        channels: 2,
    };
}

pub fn validate_audio(
    description: &RTCSessionDescription,
) -> Result<NegotiatedAudio, Box<dyn std::error::Error + Send + Sync>> {
    let parsed = description.unmarshal()?;
    if parsed.media_descriptions.len() != 1 {
        return Err("M1 requires exactly one audio section".into());
    }
    let media = &parsed.media_descriptions[0];
    if media.media_name.media != "audio"
        || media.media_name.port.value == 0
        || !media
            .media_name
            .formats
            .iter()
            .any(|format| format == "111")
    {
        return Err("M1 requires active Opus audio with payload type 111".into());
    }
    let maps: Vec<_> = media
        .attributes
        .iter()
        .filter(|a| a.key == "rtpmap")
        .filter_map(|a| a.value.as_deref())
        .filter(|v| v.split_whitespace().next() == Some("111"))
        .collect();
    if maps.len() != 1 || !maps[0].eq_ignore_ascii_case("111 opus/48000/2") {
        return Err("payload 111 must map to opus/48000/2".into());
    }
    if media.attribute("inactive").is_some()
        || media.attribute("recvonly").is_some()
        || media.attribute("sendonly").is_some()
    {
        return Err("M1 requires bidirectional audio".into());
    }
    Ok(NegotiatedAudio::OPUS)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sdp(mapping: &str, format: &str) -> RTCSessionDescription {
        RTCSessionDescription::answer(format!("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF {format}\r\na=rtpmap:{mapping}\r\na=sendrecv\r\n")).unwrap()
    }
    #[test]
    fn validates_negotiated_opus_and_rejects_mismatched_payload_or_codec() {
        assert_eq!(
            validate_audio(&sdp("111 opus/48000/2", "111")).unwrap(),
            NegotiatedAudio::OPUS
        );
        for (mapping, format) in [
            ("112 opus/48000/2", "112"),
            ("111 PCMU/8000", "111"),
            ("111 opus/16000/2", "111"),
            ("111 opus/48000/1", "111"),
            ("111 opus/48000/2", "0"),
        ] {
            assert!(validate_audio(&sdp(mapping, format)).is_err());
        }
    }
}

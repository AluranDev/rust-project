use std::sync::Arc;

use tokio::sync::mpsc::{Sender, error::TrySendError};

use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};

use audio::codec::opus::OpusCodec;

/// A decoded audio frame received from a remote peer.
#[derive(Debug)]
pub struct DecodedAudioFrame {
    pub peer_id: String,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub sample_rate: u32,
    pub channels: u16,
    pub samples: Vec<f32>,
}

/// Audio receiver statistics.
#[derive(Debug, Default, Clone, Copy)]
pub struct AudioReceiverStats {
    pub packets_received: u64,
    pub packets_decoded: u64,
    pub decode_errors: u64,
    pub frames_dropped: u64,
    pub track_errors: u64,
    pub incompatible_packets: u64,
}

/// Receives RTP packets and decodes their Opus payloads.
pub struct AudioReceiver {
    track: Arc<dyn TrackRemote>,
    peer_id: String,
    decoder: OpusCodec,
    output: Sender<DecodedAudioFrame>,
    sample_rate: u32,
    channels: u16,
}

impl AudioReceiver {
    pub fn new(
        track: Arc<dyn TrackRemote>,
        peer_id: impl Into<String>,
        output: Sender<DecodedAudioFrame>,
        sample_rate: u32,
        channels: u16,
        frame_duration_ms: u32,
    ) -> Result<Self, String> {
        let decoder = OpusCodec::new(sample_rate, channels, frame_duration_ms)?;

        Ok(Self {
            track,
            peer_id: peer_id.into(),
            decoder,
            output,
            sample_rate,
            channels,
        })
    }

    /// Continuously receives and decodes remote audio.
    ///
    /// Stops when the track ends or the output channel closes.
    pub async fn run(self) -> AudioReceiverStats {
        let (_stop, receiver) = tokio::sync::watch::channel(false);
        self.run_until(receiver).await
    }

    pub async fn run_until(
        mut self,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> AudioReceiverStats {
        let mut stats = AudioReceiverStats::default();

        loop {
            if *stop.borrow() {
                break;
            }
            let event = tokio::select! {
                event = self.track.poll() => event,
                _ = stop.changed() => break,
            };
            let Some(event) = event else {
                break;
            };

            match event {
                TrackRemoteEvent::OnRtpPacket(packet) => {
                    stats.packets_received += 1;

                    let codec = self.track.codec(packet.header.ssrc).await;
                    if packet.header.payload_type != 111
                        || !codec.is_some_and(|codec| {
                            codec.mime_type.eq_ignore_ascii_case("audio/opus")
                                && codec.clock_rate == 48000
                                && codec.channels == 2
                        })
                    {
                        stats.incompatible_packets += 1;
                        continue;
                    }
                    if packet.payload.is_empty() {
                        continue;
                    }

                    let mut pcm = vec![0.0_f32; self.decoder.max_decoded_samples()];

                    let decoded_samples = match self
                        .decoder
                        .decode_packet(packet.payload.as_ref(), &mut pcm)
                    {
                        Ok(samples) => samples,
                        Err(_) => {
                            stats.decode_errors += 1;
                            continue;
                        }
                    };

                    pcm.truncate(decoded_samples);

                    let frame = DecodedAudioFrame {
                        peer_id: self.peer_id.clone(),
                        sequence_number: packet.header.sequence_number,
                        timestamp: packet.header.timestamp,
                        ssrc: packet.header.ssrc,
                        sample_rate: self.sample_rate,
                        channels: self.channels,
                        samples: pcm,
                    };

                    match self.output.try_send(frame) {
                        Ok(()) => {
                            stats.packets_decoded += 1;
                        }

                        Err(TrySendError::Full(_)) => {
                            stats.frames_dropped += 1;
                        }

                        Err(TrySendError::Closed(_)) => {
                            break;
                        }
                    }
                }

                TrackRemoteEvent::OnError => {
                    stats.track_errors += 1;
                    break;
                }

                TrackRemoteEvent::OnEnding | TrackRemoteEvent::OnEnded => {
                    break;
                }

                _ => {}
            }
        }

        stats
    }
}

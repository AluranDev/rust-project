use std::sync::Arc;
use std::time::Duration;

use audio::capture::AudioCapture;
use audio::codec::opus::OpusCodec;
use audio::frame_processor::AudioFrameProcessor;

use crate::webrtc_connection::WebRtcConnection;

const READ_INTERVAL_MS: u64 = 2;
const MAX_OPUS_PACKET_SIZE: usize = 4000;

#[derive(Debug, Default)]
pub struct AudioSenderStats {
    pub frames_processed: u64,
    pub frames_sent: u64,
    pub encode_errors: u64,
    pub send_errors: u64,
    pub input_errors: u64,
    pub input_dropped: u64,
}

pub trait CaptureSource {
    fn generation(&self) -> u64 {
        0
    }
    fn is_current(&self, generation: u64) -> bool {
        self.generation() == generation
    }
    fn sample_rate(&self) -> u32;
    fn channels(&self) -> u16;
    fn receive_into(&mut self, output: &mut [f32]) -> usize;
    fn check_health(&self) -> Result<(), String>;
    fn take_backend_errors(&self) -> u64;
    fn take_dropped_samples(&self) -> u64;
}
impl CaptureSource for AudioCapture {
    fn sample_rate(&self) -> u32 {
        self.sample_rate()
    }
    fn channels(&self) -> u16 {
        self.channels()
    }
    fn receive_into(&mut self, output: &mut [f32]) -> usize {
        self.receive_into(output)
    }
    fn check_health(&self) -> Result<(), String> {
        self.check_health()
    }
    fn take_backend_errors(&self) -> u64 {
        self.take_backend_errors()
    }
    fn take_dropped_samples(&self) -> u64 {
        self.take_dropped_samples()
    }
}
pub fn capture_frame_samples(rate: u32, channels: u16, duration_ms: u32) -> usize {
    ((rate as usize * duration_ms as usize / 1000).max(1)) * channels as usize
}

pub struct AudioSenderWorker<C: CaptureSource = AudioCapture> {
    capture: C,
    connection: Arc<WebRtcConnection>,
    processor: AudioFrameProcessor,
    codec: OpusCodec,
    resampler: audio::format::PcmResampler,
    read_buffer: Vec<f32>,
    frame_duration: Duration,
    stats: AudioSenderStats,
}

impl<C: CaptureSource> AudioSenderWorker<C> {
    pub fn new(
        capture: C,
        connection: Arc<WebRtcConnection>,
        frame_duration_ms: u32,
    ) -> Result<Self, String> {
        let sample_rate = capture.sample_rate();
        let channels = capture.channels();

        if ![1, 2].contains(&channels) {
            return Err("Audio must be mono or stereo".into());
        }

        let processor = AudioFrameProcessor::new(48000, 2, frame_duration_ms)?;

        let codec = OpusCodec::new(48000, 2, frame_duration_ms)?;

        let read_buffer =
            vec![0.0; capture_frame_samples(sample_rate, channels, frame_duration_ms)];
        let resampler = audio::format::PcmResampler::new(sample_rate, channels, 48000, 2)?;

        Ok(Self {
            capture,
            connection,
            processor,
            codec,
            resampler,
            read_buffer,
            frame_duration: Duration::from_millis(frame_duration_ms as u64),
            stats: AudioSenderStats::default(),
        })
    }

    pub async fn run(
        mut self,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<AudioSenderStats, String> {
        let mut encoded_frames: Vec<Vec<u8>> = Vec::new();
        let mut generation = self.capture.generation();

        println!("Audio Sender Worker started.");

        'capture: loop {
            if *stop.borrow() {
                break;
            }

            self.capture
                .check_health()
                .map_err(|error| format!("input device failed: {error}"))?;
            let samples = self.capture.receive_into(&mut self.read_buffer);
            let current = self.capture.generation();
            if current != generation {
                generation = current;
                self.processor =
                    AudioFrameProcessor::new(48000, 2, self.frame_duration.as_millis() as u32)?;
                self.resampler = audio::format::PcmResampler::new(
                    self.capture.sample_rate(),
                    self.capture.channels(),
                    48000,
                    2,
                )?;
            }

            if samples == 0 {
                tokio::select! { _ = stop.changed() => break, _ = tokio::time::sleep(Duration::from_millis(READ_INTERVAL_MS)) => () }

                continue;
            }

            encoded_frames.clear();

            let mut encode_error: Option<String> = None;

            let converted = self.resampler.process(&self.read_buffer[..samples])?;
            self.processor.push(&converted, |frame| {
                let mut packet = vec![0u8; MAX_OPUS_PACKET_SIZE];
                match self.codec.encode_frame(frame, &mut packet) {
                    Ok(size) => {
                        packet.truncate(size);
                        encoded_frames.push(packet);
                    }

                    Err(error) => {
                        encode_error = Some(error);
                    }
                }
            });

            if let Some(error) = encode_error {
                self.stats.encode_errors += 1;

                return Err(format!("Opus encoding error: {error}"));
            }

            for packet in encoded_frames.drain(..) {
                if !self.capture.is_current(generation) {
                    break;
                }
                self.stats.frames_processed += 1;

                let sent = tokio::select! {
                    biased;
                    _ = stop.changed() => break 'capture,
                    result = self.connection.send_opus(&packet, self.frame_duration) => result,
                };
                match sent {
                    Ok(()) => {
                        self.stats.frames_sent += 1;
                    }

                    Err(error) => {
                        if *self.connection.subscribe_state().borrow()
                            == webrtc::peer_connection::RTCPeerConnectionState::Closed
                        {
                            break 'capture;
                        }
                        self.stats.send_errors += 1;

                        return Err(format!("WebRTC audio transmission failed: {error}"));
                    }
                }
            }

            self.stats.input_errors += self.capture.take_backend_errors();

            self.stats.input_dropped += self.capture.take_dropped_samples();
        }

        self.stats.input_errors += self.capture.take_backend_errors();

        self.stats.input_dropped += self.capture.take_dropped_samples();

        println!("Audio Sender Worker stopped.");

        Ok(self.stats)
    }
}

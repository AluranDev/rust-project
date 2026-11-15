use crate::capture::AudioCapture;
use crate::codec::opus::OpusCodec;
use crate::frame_processor::AudioFrameProcessor;
use crate::playback::AudioPlayback;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
};

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const METRICS_INTERVAL: Duration = Duration::from_secs(1);
const IDLE_SLEEP: Duration = Duration::from_millis(1);
const OPUS_PACKET_CAPACITY: usize = 4000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioMode {
    Pcm,
    OpusLoopback,
}

pub struct AudioMetrics {
    pub frames_processed: u64,
    pub input_errors: u64,
    pub input_dropped: u64,
    pub output_errors: u64,
    pub output_underruns: u64,
    pub output_dropped: u64,

    pub opus_encoded_bytes: u64,
    pub opus_encode_time_us: u64,
    pub opus_decode_time_us: u64,
}

struct WorkerStats {
    frames_processed: u64,
    opus_encoded_bytes: u64,
    opus_encode_time_us: u64,
    opus_decode_time_us: u64,
}

impl WorkerStats {
    fn new() -> Self {
        Self {
            frames_processed: 0,
            opus_encoded_bytes: 0,
            opus_encode_time_us: 0,
            opus_decode_time_us: 0,
        }
    }

    fn reset(&mut self) {
        self.frames_processed = 0;
        self.opus_encoded_bytes = 0;
        self.opus_encode_time_us = 0;
        self.opus_decode_time_us = 0;
    }
}

pub struct AudioEngine {
    stop: Arc<AtomicBool>,
    metrics_rx: Receiver<AudioMetrics>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

impl AudioEngine {
    pub fn start(
        capture: AudioCapture,
        playback: AudioPlayback,
        frame_duration_ms: u32,
    ) -> Result<Self, String> {
        Self::start_with_volume(capture, playback, frame_duration_ms, 0.15)
    }

    pub fn start_with_volume(
        capture: AudioCapture,
        playback: AudioPlayback,
        frame_duration_ms: u32,
        volume: f32,
    ) -> Result<Self, String> {
        Self::start_internal(capture, playback, frame_duration_ms, volume, AudioMode::Pcm)
    }

    pub fn start_opus_loopback(
        capture: AudioCapture,
        playback: AudioPlayback,
        frame_duration_ms: u32,
        volume: f32,
    ) -> Result<Self, String> {
        Self::start_internal(
            capture,
            playback,
            frame_duration_ms,
            volume,
            AudioMode::OpusLoopback,
        )
    }

    fn start_internal(
        capture: AudioCapture,
        playback: AudioPlayback,
        frame_duration_ms: u32,
        volume: f32,
        mode: AudioMode,
    ) -> Result<Self, String> {
        if capture.sample_rate() != playback.sample_rate() {
            return Err("Input/output sample rate mismatch".into());
        }

        if capture.channels() != playback.channels() {
            return Err("Input/output channel mismatch".into());
        }

        if !volume.is_finite() || !(0.0..=1.0).contains(&volume) {
            return Err("Volume must be between 0.0 and 1.0".into());
        }

        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);

        let (metrics_tx, metrics_rx) = mpsc::sync_channel(2);

        let worker = thread::Builder::new()
            .name("audio-worker".into())
            .spawn(move || {
                Self::worker_loop(
                    capture,
                    playback,
                    frame_duration_ms,
                    volume,
                    mode,
                    worker_stop,
                    metrics_tx,
                )
            })
            .map_err(|e| e.to_string())?;

        Ok(Self {
            stop,
            metrics_rx,
            worker: Some(worker),
        })
    }

    fn worker_loop(
        mut capture: AudioCapture,
        mut playback: AudioPlayback,
        frame_duration_ms: u32,
        volume: f32,
        mode: AudioMode,
        stop: Arc<AtomicBool>,
        metrics_tx: SyncSender<AudioMetrics>,
    ) -> Result<(), String> {
        let sample_rate = capture.sample_rate();
        let channels = capture.channels();

        let mut processor = AudioFrameProcessor::new(sample_rate, channels, frame_duration_ms)?;

        let frame_samples =
            (sample_rate as usize * channels as usize * frame_duration_ms as usize) / 1000;

        let mut capture_buffer = vec![0.0_f32; frame_samples];

        // One persistent encoder/decoder per session.
        let mut codec = match mode {
            AudioMode::Pcm => None,

            AudioMode::OpusLoopback => {
                Some(OpusCodec::new(sample_rate, channels, frame_duration_ms)?)
            }
        };

        // Reusable buffers.
        let mut opus_packet = vec![0_u8; OPUS_PACKET_CAPACITY];

        let mut decoded_buffer = codec
            .as_ref()
            .map(|codec| vec![0.0_f32; codec.max_decoded_samples()])
            .unwrap_or_default();

        let mut pcm_output_buffer = vec![0.0_f32; frame_samples];

        let mut stats = WorkerStats::new();
        let mut last_report = Instant::now();

        while !stop.load(Ordering::Acquire) {
            let count = capture.receive_into(&mut capture_buffer);

            if count > capture_buffer.len() {
                return Err("Capture returned an invalid sample count".into());
            }

            if count == 0 {
                thread::sleep(IDLE_SLEEP);
            } else {
                let mut codec_error: Option<String> = None;

                processor.push(&capture_buffer[..count], |frame| {
                    if codec_error.is_some() {
                        return;
                    }

                    match codec.as_mut() {
                        None => {
                            // PCM loopback.
                            if frame.len() > pcm_output_buffer.len() {
                                codec_error = Some("PCM frame exceeds output buffer".into());
                                return;
                            }

                            pcm_output_buffer[..frame.len()].copy_from_slice(frame);

                            for sample in &mut pcm_output_buffer[..frame.len()] {
                                *sample *= volume;
                            }

                            playback.write(&pcm_output_buffer[..frame.len()]);
                        }

                        Some(codec) => {
                            // PCM -> Opus.
                            let encode_start = Instant::now();

                            let encoded_size = match codec.encode_frame(frame, &mut opus_packet) {
                                Ok(size) => size,
                                Err(error) => {
                                    codec_error = Some(error);
                                    return;
                                }
                            };

                            stats.opus_encode_time_us += encode_start.elapsed().as_micros() as u64;

                            stats.opus_encoded_bytes += encoded_size as u64;

                            // Opus -> PCM.
                            let decode_start = Instant::now();

                            let decoded_size = match codec
                                .decode_packet(&opus_packet[..encoded_size], &mut decoded_buffer)
                            {
                                Ok(size) => size,
                                Err(error) => {
                                    codec_error = Some(error);
                                    return;
                                }
                            };

                            stats.opus_decode_time_us += decode_start.elapsed().as_micros() as u64;

                            // Apply playback volume after decoding.
                            for sample in &mut decoded_buffer[..decoded_size] {
                                *sample *= volume;
                            }

                            playback.write(&decoded_buffer[..decoded_size]);
                        }
                    }

                    stats.frames_processed += 1;
                });

                if let Some(error) = codec_error {
                    return Err(error);
                }
            }

            // Periodic metrics.
            if last_report.elapsed() >= METRICS_INTERVAL {
                let metrics = Self::collect_metrics(&mut capture, &mut playback, &stats);

                // Never block the audio worker for metrics.
                let _ = metrics_tx.try_send(metrics);

                stats.reset();
                last_report = Instant::now();
            }
        }

        // Final metrics.
        let final_metrics = Self::collect_metrics(&mut capture, &mut playback, &stats);

        let _ = metrics_tx.try_send(final_metrics);

        Ok(())
    }

    fn collect_metrics(
        capture: &mut AudioCapture,
        playback: &mut AudioPlayback,
        stats: &WorkerStats,
    ) -> AudioMetrics {
        AudioMetrics {
            frames_processed: stats.frames_processed,

            input_errors: capture.take_backend_errors(),
            input_dropped: capture.take_dropped_samples(),

            output_errors: playback.take_backend_errors(),
            output_underruns: playback.take_underrun_callbacks(),
            output_dropped: playback.take_dropped_samples(),

            opus_encoded_bytes: stats.opus_encoded_bytes,
            opus_encode_time_us: stats.opus_encode_time_us,
            opus_decode_time_us: stats.opus_decode_time_us,
        }
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<AudioMetrics, RecvTimeoutError> {
        self.metrics_rx.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<AudioMetrics, TryRecvError> {
        self.metrics_rx.try_recv()
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.stop.store(true, Ordering::Release);

        if let Some(worker) = self.worker.take() {
            match worker.join() {
                Ok(result) => result,
                Err(_) => Err("Audio worker panicked".into()),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for AudioEngine {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);

        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

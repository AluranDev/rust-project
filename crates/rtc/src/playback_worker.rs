use tokio::sync::mpsc::Receiver;

use audio::playback::AudioPlayback;

use crate::audio_receiver::DecodedAudioFrame;

pub trait PlaybackSink {
    fn generation(&self) -> u64 {
        0
    }
    fn sample_rate(&self) -> u32;
    fn channels(&self) -> u16;
    fn check_health(&self) -> Result<(), String>;
    fn write(&mut self, samples: &[f32]) -> usize;
    fn take_underrun_callbacks(&self) -> u64;
    fn take_backend_errors(&self) -> u64;
    fn take_dropped_samples(&mut self) -> u64;
}
impl PlaybackSink for AudioPlayback {
    fn sample_rate(&self) -> u32 {
        self.sample_rate()
    }
    fn channels(&self) -> u16 {
        self.channels()
    }
    fn check_health(&self) -> Result<(), String> {
        self.check_health()
    }
    fn write(&mut self, samples: &[f32]) -> usize {
        self.write(samples)
    }
    fn take_underrun_callbacks(&self) -> u64 {
        self.take_underrun_callbacks()
    }
    fn take_backend_errors(&self) -> u64 {
        self.take_backend_errors()
    }
    fn take_dropped_samples(&mut self) -> u64 {
        self.take_dropped_samples()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlaybackWorkerStats {
    pub frames_received: u64,
    pub frames_written: u64,
    pub frames_dropped: u64,
    pub incompatible_frames: u64,
    pub invalid_frames: u64,
    pub samples_written: u64,
    pub underrun_callbacks: u64,
    pub backend_errors: u64,
    pub dropped_samples: u64,
}

pub struct PlaybackWorker<P: PlaybackSink = AudioPlayback> {
    playback: P,
    input: Receiver<DecodedAudioFrame>,
    volume: f32,
}

impl<P: PlaybackSink> PlaybackWorker<P> {
    pub fn new(playback: P, input: Receiver<DecodedAudioFrame>) -> Self {
        Self {
            playback,
            input,
            volume: 1.0,
        }
    }

    pub fn with_volume(mut self, volume: f32) -> Result<Self, String> {
        if !volume.is_finite() || !(0.0..=1.0).contains(&volume) {
            return Err("volume must be between 0 and 1".into());
        }
        self.volume = volume;
        Ok(self)
    }
    pub async fn run(self) -> Result<PlaybackWorkerStats, String> {
        let (_stop, stopped) = tokio::sync::watch::channel(false);
        self.run_until(stopped).await
    }
    pub async fn run_until(
        mut self,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> Result<PlaybackWorkerStats, String> {
        let mut stats = PlaybackWorkerStats::default();
        let mut resampler = None;
        let mut source_format = None;
        let mut generation = self.playback.generation();

        loop {
            if *stop.borrow() {
                break;
            }
            self.playback
                .check_health()
                .map_err(|error| format!("output device failed: {error}"))?;
            let frame = tokio::select! {
                _ = stop.changed() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => continue,
                frame = self.input.recv() => frame,
            };
            let Some(frame) = frame else {
                break;
            };
            let current = self.playback.generation();
            if current != generation {
                generation = current;
                // Drop queued audio from the previous output epoch, including this frame.
                for _ in 0..self.input.len() {
                    let _ = self.input.try_recv();
                }
                resampler = None;
                source_format = None;
                continue;
            }
            stats.frames_received += 1;

            // PCM must contain complete interleaved channel frames.
            if frame.channels == 0
                || frame.samples.is_empty()
                || frame.samples.len() % frame.channels as usize != 0
            {
                stats.invalid_frames += 1;
                continue;
            }

            if source_format != Some((frame.sample_rate, frame.channels)) {
                resampler = audio::format::PcmResampler::new(
                    frame.sample_rate,
                    frame.channels,
                    self.playback.sample_rate(),
                    self.playback.channels(),
                )
                .ok();
                source_format = Some((frame.sample_rate, frame.channels));
            }
            let Some(converter) = resampler.as_mut() else {
                stats.incompatible_frames += 1;
                continue;
            };
            let mut converted = match converter.process(&frame.samples) {
                Ok(samples) => samples,
                Err(_) => {
                    stats.incompatible_frames += 1;
                    continue;
                }
            };
            for sample in &mut converted {
                *sample *= self.volume;
            }
            let expected_samples = converted.len();
            let written = self.playback.write(&converted);

            stats.samples_written += written as u64;

            if written == expected_samples {
                stats.frames_written += 1;
            } else {
                stats.frames_dropped += 1;
            }
        }

        // Collect final playback statistics.
        stats.underrun_callbacks = self.playback.take_underrun_callbacks();

        stats.backend_errors = self.playback.take_backend_errors();

        stats.dropped_samples = self.playback.take_dropped_samples();

        Ok(stats)
    }
}

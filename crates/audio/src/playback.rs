use cpal::traits::{DeviceTrait, StreamTrait};
use cpal::{SampleFormat, SizedSample, Stream, StreamConfig};

use ringbuf::{
    HeapCons, HeapProd, HeapRb,
    traits::{Consumer, Observer, Producer, Split},
};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub struct AudioPlayback {
    _stream: Stream,
    flush: Arc<AtomicU64>,
    health: crate::health::DeviceHealth,
    producer: HeapProd<f32>,
    sample_rate: u32,
    channels: u16,
    underrun_callbacks: Arc<AtomicU64>,
    backend_errors: Arc<AtomicU64>,
    dropped_samples: u64,
    written_samples: u64,
}

impl AudioPlayback {
    pub fn new(device: cpal::Device) -> Result<Self, String> {
        let supported_config = device.default_output_config().map_err(|e| e.to_string())?;

        let sample_format = supported_config.sample_format();
        let config: StreamConfig = supported_config.into();

        let sample_rate = config.sample_rate;
        let channels = config.channels;

        let capacity = (sample_rate as usize * 250 / 1000).max(1) * channels as usize;

        let ring = HeapRb::<f32>::new(capacity);
        let (producer, consumer) = ring.split();

        let underrun_callbacks = Arc::new(AtomicU64::new(0));
        let backend_errors = Arc::new(AtomicU64::new(0));

        let health = crate::health::DeviceHealth::default();
        let flush = Arc::new(AtomicU64::new(0));
        let stream = match sample_format {
            SampleFormat::F32 => Self::build_stream::<f32, _>(
                &device,
                &config,
                consumer,
                |sample| sample,
                Arc::clone(&underrun_callbacks),
                Arc::clone(&backend_errors),
                health.clone(),
                flush.clone(),
            )?,

            SampleFormat::I16 => Self::build_stream::<i16, _>(
                &device,
                &config,
                consumer,
                |sample| (sample.clamp(-1.0, 1.0) * i16::MAX as f32) as i16,
                Arc::clone(&underrun_callbacks),
                Arc::clone(&backend_errors),
                health.clone(),
                flush.clone(),
            )?,

            SampleFormat::U16 => Self::build_stream::<u16, _>(
                &device,
                &config,
                consumer,
                |sample| ((sample.clamp(-1.0, 1.0) + 1.0) * 0.5 * u16::MAX as f32) as u16,
                Arc::clone(&underrun_callbacks),
                Arc::clone(&backend_errors),
                health.clone(),
                flush.clone(),
            )?,

            _ => return Err("Unsupported output format".into()),
        };

        stream.play().map_err(|e| e.to_string())?;

        Ok(Self {
            _stream: stream,
            flush,
            health,
            producer,
            sample_rate,
            channels,
            underrun_callbacks,
            backend_errors,
            dropped_samples: 0,
            written_samples: 0,
        })
    }

    fn build_stream<T, F>(
        device: &cpal::Device,
        config: &StreamConfig,
        mut consumer: HeapCons<f32>,
        convert: F,
        underrun_callbacks: Arc<AtomicU64>,
        backend_errors: Arc<AtomicU64>,
        health: crate::health::DeviceHealth,
        flush: Arc<AtomicU64>,
    ) -> Result<Stream, String>
    where
        T: SizedSample + 'static,
        F: Fn(f32) -> T + Send + 'static,
    {
        let channels = config.channels as usize;
        let mut consumed = 0u64;
        device
            .build_output_stream(
                config.clone(),
                move |output: &mut [T], _| {
                    discard_before(&mut consumer, &mut consumed, flush.load(Ordering::SeqCst));
                    let mut underrun = false;

                    for frame in output.chunks_mut(channels) {
                        let available =
                            self::complete_frame_available(consumer.occupied_len(), channels);
                        if !available {
                            underrun = true;
                        }
                        for output_sample in frame {
                            let sample = if available {
                                consumed += 1;
                                consumer.try_pop().unwrap_or(0.0)
                            } else {
                                0.0
                            };
                            *output_sample = convert(sample);
                        }
                    }

                    if underrun {
                        underrun_callbacks.fetch_add(1, Ordering::Relaxed);
                    }
                },
                move |error| {
                    health.fail(error.to_string());
                    backend_errors.fetch_add(1, Ordering::Relaxed);
                },
                None,
            )
            .map_err(|e| e.to_string())
    }

    // Write captured samples into the playback buffer.
    pub fn write(&mut self, samples: &[f32]) -> usize {
        let count = (self.producer.vacant_len().min(samples.len()) / self.channels as usize)
            * self.channels as usize;
        let written = self.producer.push_slice(&samples[..count]);

        self.dropped_samples += (samples.len() - written) as u64;
        self.written_samples += written as u64;

        written
    }

    pub fn discard_pending(&mut self) {
        self.flush.store(self.written_samples, Ordering::SeqCst);
    }

    pub fn check_health(&self) -> Result<(), String> {
        self.health.check()
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn channels(&self) -> u16 {
        self.channels
    }

    pub fn take_underrun_callbacks(&self) -> u64 {
        self.underrun_callbacks.swap(0, Ordering::Relaxed)
    }

    pub fn take_backend_errors(&self) -> u64 {
        self.backend_errors.swap(0, Ordering::Relaxed)
    }

    pub fn take_dropped_samples(&mut self) -> u64 {
        std::mem::take(&mut self.dropped_samples)
    }
}

fn complete_frame_available(samples: usize, channels: usize) -> bool {
    samples >= channels
}

fn discard_before(consumer: &mut HeapCons<f32>, consumed: &mut u64, cutoff: u64) {
    let count = (cutoff.saturating_sub(*consumed) as usize).min(consumer.occupied_len());
    *consumed += consumer.skip(count) as u64;
}
#[cfg(test)]
mod flush_tests {
    use super::*;
    #[test]
    fn delayed_flush_discards_only_samples_queued_at_request_time() {
        let (mut producer, mut consumer) = HeapRb::<f32>::new(10).split();
        producer.push_slice(&[1.0, 1.0]);
        let cutoff = 2;
        producer.push_slice(&[2.0, 2.0]);
        let mut consumed = 0;
        discard_before(&mut consumer, &mut consumed, cutoff);
        assert_eq!(consumed, 2);
        assert_eq!(consumer.try_pop(), Some(2.0));
        assert_eq!(consumer.try_pop(), Some(2.0));
    }
}

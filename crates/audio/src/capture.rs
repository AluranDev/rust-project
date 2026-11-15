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

const BUFFER_DURATION_MS: usize = 250;

pub struct AudioCapture {
    _stream: Stream,
    health: crate::health::DeviceHealth,
    consumer: HeapCons<f32>,
    sample_rate: u32,
    channels: u16,
    backend_errors: Arc<AtomicU64>,
    dropped_samples: Arc<AtomicU64>,
}

impl AudioCapture {
    pub fn new(device: cpal::Device) -> Result<Self, String> {
        let supported_config = device.default_input_config().map_err(|e| e.to_string())?;

        let sample_format = supported_config.sample_format();
        let config: StreamConfig = supported_config.into();

        let sample_rate = config.sample_rate;
        let channels = config.channels;

        let capacity =
            (sample_rate as usize * BUFFER_DURATION_MS / 1000).max(1) * channels as usize;

        let ring = HeapRb::<f32>::new(capacity);
        let (producer, consumer) = ring.split();

        let backend_errors = Arc::new(AtomicU64::new(0));
        let dropped_samples = Arc::new(AtomicU64::new(0));

        let health = crate::health::DeviceHealth::default();
        let stream = match sample_format {
            SampleFormat::F32 => Self::build_stream::<f32, _>(
                &device,
                &config,
                producer,
                |sample| *sample,
                Arc::clone(&backend_errors),
                Arc::clone(&dropped_samples),
                health.clone(),
            )?,

            SampleFormat::I16 => Self::build_stream::<i16, _>(
                &device,
                &config,
                producer,
                |sample| *sample as f32 / 32768.0,
                Arc::clone(&backend_errors),
                Arc::clone(&dropped_samples),
                health.clone(),
            )?,

            SampleFormat::U16 => Self::build_stream::<u16, _>(
                &device,
                &config,
                producer,
                |sample| (*sample as f32 / 65535.0) * 2.0 - 1.0,
                Arc::clone(&backend_errors),
                Arc::clone(&dropped_samples),
                health.clone(),
            )?,

            _ => return Err("Unsupported audio format".into()),
        };

        stream.play().map_err(|e| e.to_string())?;

        Ok(Self {
            _stream: stream,
            health,
            consumer,
            sample_rate,
            channels,
            backend_errors,
            dropped_samples,
        })
    }

    fn build_stream<T, F>(
        device: &cpal::Device,
        config: &StreamConfig,
        mut producer: HeapProd<f32>,
        convert: F,
        backend_errors: Arc<AtomicU64>,
        dropped_samples: Arc<AtomicU64>,
        health: crate::health::DeviceHealth,
    ) -> Result<Stream, String>
    where
        T: SizedSample + 'static,
        F: Fn(&T) -> f32 + Send + 'static,
    {
        let channels = config.channels as usize;
        device
            .build_input_stream(
                config.clone(),
                move |data: &[T], _| {
                    let count = (producer.vacant_len().min(data.len()) / channels) * channels;
                    let written = producer.push_iter(data[..count].iter().map(&convert));

                    let dropped = data.len() - written;

                    if dropped > 0 {
                        dropped_samples.fetch_add(dropped as u64, Ordering::Relaxed);
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

    pub fn receive_into(&mut self, output: &mut [f32]) -> usize {
        pop_frames(&mut self.consumer, output, self.channels as usize)
    }

    pub fn check_health(&self) -> Result<(), String> {
        self.health.check()
    }

    /// Discard samples captured before the replacement becomes active.
    pub fn discard_pending(&mut self) {
        let count = self.consumer.occupied_len() / self.channels as usize * self.channels as usize;
        let _ = self.consumer.skip(count);
    }
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn channels(&self) -> u16 {
        self.channels
    }

    pub fn take_backend_errors(&self) -> u64 {
        self.backend_errors.swap(0, Ordering::Relaxed)
    }

    pub fn take_dropped_samples(&self) -> u64 {
        self.dropped_samples.swap(0, Ordering::Relaxed)
    }
}

fn pop_frames(consumer: &mut HeapCons<f32>, output: &mut [f32], channels: usize) -> usize {
    let count = (consumer.occupied_len().min(output.len()) / channels) * channels;
    consumer.pop_slice(&mut output[..count])
}
#[cfg(test)]
mod alignment_tests {
    use super::*;
    #[test]
    fn capture_retains_partial_frames_until_all_channels_arrive() {
        let (mut producer, mut consumer) = HeapRb::<f32>::new(10).split();
        producer.push_slice(&[1.0, 2.0, 3.0]);
        let mut output = [0.0; 5];
        assert_eq!(pop_frames(&mut consumer, &mut output, 2), 2);
        assert_eq!(&output[..2], &[1.0, 2.0]);
        assert_eq!(pop_frames(&mut consumer, &mut output, 2), 0);
        producer.push_slice(&[4.0]);
        assert_eq!(pop_frames(&mut consumer, &mut output, 2), 2);
        assert_eq!(&output[..2], &[3.0, 4.0]);
    }
}

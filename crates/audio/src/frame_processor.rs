pub struct AudioFrameProcessor {
    frame: Vec<f32>,
    filled: usize,
    sample_rate: u32,
    channels: u16,
    frame_duration_ms: u32,
}

impl AudioFrameProcessor {
    pub fn new(sample_rate: u32, channels: u16, frame_duration_ms: u32) -> Result<Self, String> {
        if sample_rate == 0 || channels == 0 {
            return Err("Invalid audio configuration".into());
        }

        if ![10, 20, 40, 60].contains(&frame_duration_ms) {
            return Err("Unsupported frame duration".into());
        }

        let samples_per_channel = (sample_rate as u64 * frame_duration_ms as u64) / 1000;

        if samples_per_channel == 0 || (sample_rate as u64 * frame_duration_ms as u64) % 1000 != 0 {
            return Err("Invalid frame configuration".into());
        }

        let frame_samples = samples_per_channel as usize * channels as usize;

        Ok(Self {
            frame: vec![0.0; frame_samples],
            filled: 0,
            sample_rate,
            channels,
            frame_duration_ms,
        })
    }

    pub fn push<F>(&mut self, samples: &[f32], mut on_frame: F)
    where
        F: FnMut(&mut [f32]),
    {
        for &sample in samples {
            self.frame[self.filled] = sample;
            self.filled += 1;

            if self.filled == self.frame.len() {
                on_frame(&mut self.frame);
                self.filled = 0;
            }
        }
    }

    pub fn frame_samples(&self) -> usize {
        self.frame.len()
    }

    pub fn samples_per_channel(&self) -> usize {
        self.frame.len() / self.channels as usize
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    pub fn channels(&self) -> u16 {
        self.channels
    }

    pub fn frame_duration_ms(&self) -> u32 {
        self.frame_duration_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_20ms_stereo_frame() {
        let processor = AudioFrameProcessor::new(48000, 2, 20).unwrap();

        assert_eq!(processor.frame_samples(), 1920);
        assert_eq!(processor.samples_per_channel(), 960);
    }

    #[test]
    fn processes_complete_frames() {
        let mut processor = AudioFrameProcessor::new(48000, 2, 20).unwrap();

        let samples = vec![0.5; 3840];
        let mut frames = 0;

        processor.push(&samples, |_| {
            frames += 1;
        });

        assert_eq!(frames, 2);
    }

    #[test]
    fn preserves_partial_frames() {
        let mut processor = AudioFrameProcessor::new(48000, 2, 20).unwrap();

        let first = vec![0.5; 1000];
        let second = vec![0.5; 920];

        let mut frames = 0;

        processor.push(&first, |_| {
            frames += 1;
        });

        assert_eq!(frames, 0);

        processor.push(&second, |_| {
            frames += 1;
        });

        assert_eq!(frames, 1);
    }
}

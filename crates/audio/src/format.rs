/// Convert complete interleaved PCM frames between mono/stereo device formats.
/// Linear interpolation is intended for diagnostic voice use; it is not an
/// anti-aliasing resampler and is unsuitable for high-quality downsampling.
pub fn convert_pcm(
    input: &[f32],
    input_rate: u32,
    input_channels: u16,
    output_rate: u32,
    output_channels: u16,
) -> Result<Vec<f32>, String> {
    if input_rate == 0
        || output_rate == 0
        || ![1, 2].contains(&input_channels)
        || ![1, 2].contains(&output_channels)
    {
        return Err("PCM requires nonzero sample rates and one or two channels".into());
    }
    if input.len() % input_channels as usize != 0 {
        return Err("incomplete interleaved PCM frame".into());
    }
    let frames = input.len() / input_channels as usize;
    if frames == 0 {
        return Ok(Vec::new());
    }
    let output_frames = (frames as u64 * output_rate as u64 / input_rate as u64) as usize;
    let mut output = Vec::with_capacity(output_frames * output_channels as usize);
    for i in 0..output_frames {
        let position = i as f64 * input_rate as f64 / output_rate as f64;
        let left = (position as usize).min(frames - 1);
        let right = (left + 1).min(frames - 1);
        let fraction = (position - left as f64) as f32;
        let sample =
            |frame: usize, channel: usize| input[frame * input_channels as usize + channel];
        let channel = |c: usize| sample(left, c) + (sample(right, c) - sample(left, c)) * fraction;
        let l = channel(0);
        let r = if input_channels == 2 { channel(1) } else { l };
        if output_channels == 1 {
            output.push((l + r) * 0.5);
        } else {
            output.extend_from_slice(&[l, r]);
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn converts_channels_and_rate() {
        let mono = vec![0.5; 960];
        let stereo = convert_pcm(&mono, 48000, 1, 44100, 2).unwrap();
        assert_eq!(stereo.len(), 1764);
        assert!(stereo.iter().all(|s| *s == 0.5));
        assert_eq!(
            convert_pcm(&[1.0, -1.0], 48000, 2, 48000, 1).unwrap(),
            vec![0.0]
        );
    }
    #[test]
    fn rejects_invalid_formats() {
        assert!(convert_pcm(&[0.0], 48000, 2, 48000, 1).is_err());
        assert!(convert_pcm(&[], 0, 1, 48000, 1).is_err());
        assert!(convert_pcm(&[], 48000, 6, 48000, 2).is_err());
    }
}
/// Streaming area resampler. Each input sample is treated as a zero-order
/// hold and integrated over output intervals. The area filter suppresses
/// high frequencies before downsampling; phase and residual area persist.
pub struct PcmResampler {
    input_rate: u32,
    output_rate: u32,
    input_channels: u16,
    output_channels: u16,
    phase: u64,
    sum: [f64; 2],
}
impl PcmResampler {
    pub fn new(
        input_rate: u32,
        input_channels: u16,
        output_rate: u32,
        output_channels: u16,
    ) -> Result<Self, String> {
        if input_rate == 0
            || output_rate == 0
            || ![1, 2].contains(&input_channels)
            || ![1, 2].contains(&output_channels)
        {
            return Err("PCM requires nonzero sample rates and one or two channels".into());
        }
        Ok(Self {
            input_rate,
            output_rate,
            input_channels,
            output_channels,
            phase: 0,
            sum: [0.0; 2],
        })
    }
    pub fn process(&mut self, input: &[f32]) -> Result<Vec<f32>, String> {
        if input.len() % self.input_channels as usize != 0 {
            return Err("incomplete interleaved PCM frame".into());
        }
        let mut output = Vec::new();
        for frame in input.chunks_exact(self.input_channels as usize) {
            let left = frame[0] as f64;
            let right = if self.input_channels == 2 {
                frame[1] as f64
            } else {
                left
            };
            let samples = if self.output_channels == 1 {
                [(left + right) * 0.5, 0.0]
            } else {
                [left, right]
            };
            let mut remaining = self.output_rate as u64;
            while remaining > 0 {
                let amount = remaining.min(self.input_rate as u64 - self.phase);
                for c in 0..self.output_channels as usize {
                    self.sum[c] += samples[c] * amount as f64;
                }
                self.phase += amount;
                remaining -= amount;
                if self.phase == self.input_rate as u64 {
                    for c in 0..self.output_channels as usize {
                        output.push((self.sum[c] / self.input_rate as f64) as f32);
                        self.sum[c] = 0.0;
                    }
                    self.phase = 0;
                }
            }
        }
        Ok(output)
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    #[test]
    fn fractional_downsampling_is_independent_of_block_boundaries() {
        let input: Vec<f32> = (0..48000).map(|i| (i as f32 * 0.073).sin()).collect();
        let mut full = PcmResampler::new(48000, 1, 11025, 1).unwrap();
        let expected = full.process(&input).unwrap();
        let mut split = PcmResampler::new(48000, 1, 11025, 1).unwrap();
        let actual: Vec<_> = input
            .chunks(137)
            .flat_map(|part| split.process(part).unwrap())
            .collect();
        assert_eq!(expected.len(), 11025);
        assert_eq!(expected, actual);
    }
    #[test]
    fn downsampling_attenuates_high_frequency() {
        let input: Vec<f32> = (0..48000)
            .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
            .collect();
        let out = PcmResampler::new(48000, 1, 11025, 1)
            .unwrap()
            .process(&input)
            .unwrap();
        assert!(out.iter().map(|s| s.abs()).sum::<f32>() / (out.len() as f32) < 0.15);
    }
}

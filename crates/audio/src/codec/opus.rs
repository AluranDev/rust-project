use ::opus::{Application, Channels, Decoder, Encoder};

const MAX_PACKET_SIZE: usize = 4000;
const MAX_FRAME_DURATION_MS: usize = 120;

pub struct OpusCodec {
    encoder: Encoder,
    decoder: Decoder,

    sample_rate: u32,
    channels: u16,
    frame_duration_ms: u32,

    expected_frame_samples: usize,
    max_decoded_samples: usize,
}

impl OpusCodec {
    pub fn new(sample_rate: u32, channels: u16, frame_duration_ms: u32) -> Result<Self, String> {
        if ![8000, 12000, 16000, 24000, 48000].contains(&sample_rate) {
            return Err("Unsupported Opus sample rate".into());
        }

        if ![1, 2].contains(&channels) {
            return Err("Opus supports mono or stereo".into());
        }

        if ![10, 20, 40, 60].contains(&frame_duration_ms) {
            return Err("Unsupported Opus frame duration".into());
        }

        let opus_channels = match channels {
            1 => Channels::Mono,
            2 => Channels::Stereo,
            _ => unreachable!(),
        };

        let encoder = Encoder::new(sample_rate, opus_channels, Application::Voip)
            .map_err(|e| format!("Opus encoder initialization failed: {e}"))?;

        let decoder = Decoder::new(sample_rate, opus_channels)
            .map_err(|e| format!("Opus decoder initialization failed: {e}"))?;

        let samples_per_channel = (sample_rate as usize * frame_duration_ms as usize) / 1000;

        let expected_frame_samples = samples_per_channel * channels as usize;

        let max_decoded_samples =
            (sample_rate as usize * MAX_FRAME_DURATION_MS / 1000) * channels as usize;

        Ok(Self {
            encoder,
            decoder,
            sample_rate,
            channels,
            frame_duration_ms,
            expected_frame_samples,
            max_decoded_samples,
        })
    }

    pub fn encode_frame(&mut self, pcm: &[f32], output: &mut [u8]) -> Result<usize, String> {
        if pcm.len() != self.expected_frame_samples {
            return Err(format!(
                "Invalid PCM frame size: expected {}, got {}",
                self.expected_frame_samples,
                pcm.len()
            ));
        }

        if output.len() < MAX_PACKET_SIZE {
            return Err(format!(
                "Opus output buffer must have at least {} bytes",
                MAX_PACKET_SIZE
            ));
        }

        self.encoder
            .encode_float(pcm, output)
            .map_err(|e| format!("Opus encoding failed: {e}"))
    }

    pub fn decode_packet(&mut self, packet: &[u8], output: &mut [f32]) -> Result<usize, String> {
        if output.len() < self.max_decoded_samples {
            return Err(format!(
                "PCM output buffer must have at least {} samples",
                self.max_decoded_samples
            ));
        }

        let samples_per_channel = self
            .decoder
            .decode_float(packet, output, false)
            .map_err(|e| format!("Opus decoding failed: {e}"))?;

        Ok(samples_per_channel * self.channels as usize)
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

    pub fn frame_samples(&self) -> usize {
        self.expected_frame_samples
    }

    pub fn max_decoded_samples(&self) -> usize {
        self.max_decoded_samples
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_stereo_codec() {
        let codec = OpusCodec::new(48000, 2, 20).unwrap();

        assert_eq!(codec.sample_rate(), 48000);
        assert_eq!(codec.channels(), 2);
        assert_eq!(codec.frame_duration_ms(), 20);
        assert_eq!(codec.frame_samples(), 1920);
    }

    #[test]
    fn encodes_and_decodes_stereo_frame() {
        let mut codec = OpusCodec::new(48000, 2, 20).unwrap();

        let mut input = vec![0.0_f32; 1920];

        // Generate a 440 Hz test tone.
        for i in 0..960 {
            let sample = (std::f32::consts::TAU * 440.0 * i as f32 / 48000.0).sin() * 0.1;

            input[i * 2] = sample;
            input[i * 2 + 1] = sample;
        }

        let mut packet = vec![0_u8; MAX_PACKET_SIZE];

        let encoded_size = codec.encode_frame(&input, &mut packet).unwrap();

        assert!(encoded_size > 0);

        let mut decoded = vec![0.0_f32; codec.max_decoded_samples()];

        let decoded_size = codec
            .decode_packet(&packet[..encoded_size], &mut decoded)
            .unwrap();

        assert_eq!(decoded_size, 1920);

        assert!(
            decoded[..decoded_size]
                .iter()
                .all(|sample| sample.is_finite())
        );
    }

    #[test]
    fn rejects_invalid_frame_size() {
        let mut codec = OpusCodec::new(48000, 2, 20).unwrap();

        let input = vec![0.0_f32; 100];

        let mut packet = vec![0_u8; MAX_PACKET_SIZE];

        assert!(codec.encode_frame(&input, &mut packet).is_err());
    }
}

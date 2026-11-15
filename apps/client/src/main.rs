mod console;
mod device_audio;
mod network_audio;
use std::time::{Duration, Instant};

use audio::capture::AudioCapture;
use audio::devices::list_input_devices;
use audio::engine::AudioEngine;
use audio::playback::AudioPlayback;

use cpal::traits::{DeviceTrait, HostTrait};

const FRAME_DURATION_MS: u32 = 20;
const VOLUME: f32 = 0.15;
const TEST_DURATION_SECS: u64 = 30;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("synthetic") {
        let peer = args
            .get(2)
            .ok_or("usage: voice-client synthetic PEER ROOM")?;
        let room = args
            .get(3)
            .ok_or("usage: voice-client synthetic PEER ROOM")?;
        return tokio::runtime::Runtime::new()?
            .block_on(network_audio::synthetic(peer, room))
            .map_err(|e| e.to_string().into());
    }
    if args.get(1).map(String::as_str) == Some("voice") {
        let peer = args.get(2).ok_or("usage: voice-client voice PEER ROOM")?;
        let room = args.get(3).ok_or("usage: voice-client voice PEER ROOM")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return tokio::task::LocalSet::new()
            .block_on(&runtime, network_audio::voice(peer, room))
            .map_err(|e| e.to_string().into());
    }
    println!("Voice App - Opus Audio Engine");

    let host = cpal::default_host();

    // List available input devices.
    let input_devices = list_input_devices();

    let input_device = if let Ok(name) = std::env::var("VOICE_INPUT_DEVICE") {
        input_devices
            .into_iter()
            .find(|device| {
                device
                    .description()
                    .map(|description| description.name().contains(&name))
                    .unwrap_or(false)
            })
            .ok_or_else(|| format!("Input device matching {name:?} not found"))?
    } else {
        host.default_input_device()
            .ok_or("No default input device")?
    };
    let output_device = if let Ok(name) = std::env::var("VOICE_OUTPUT_DEVICE") {
        host.output_devices()?
            .find(|device| {
                device
                    .description()
                    .map(|description| description.name().contains(&name))
                    .unwrap_or(false)
            })
            .ok_or_else(|| format!("Output device matching {name:?} not found"))?
    } else {
        host.default_output_device()
            .ok_or("No default output device")?
    };

    println!("\nSelected input: {}", input_device.description()?.name());

    println!("Selected output: {}", output_device.description()?.name());

    // Initialize audio devices.
    let capture = AudioCapture::new(input_device).map_err(std::io::Error::other)?;

    let playback = AudioPlayback::new(output_device).map_err(std::io::Error::other)?;

    // Validate audio configuration.
    if capture.sample_rate() != 48000 {
        return Err(format!("Expected 48000 Hz input, got {} Hz", capture.sample_rate()).into());
    }

    println!("\nAudio Engine:");
    println!("Sample rate: {} Hz", capture.sample_rate());
    println!("Channels: {}", capture.channels());
    println!("Frame duration: {} ms", FRAME_DURATION_MS);
    println!("Volume: {:.0}%", VOLUME * 100.0);

    // Start Opus loopback.
    let mut engine = AudioEngine::start_opus_loopback(capture, playback, FRAME_DURATION_MS, VOLUME)
        .map_err(std::io::Error::other)?;

    println!("\nOpus Loopback active.");
    println!("Speak into your microphone.");
    println!("Running for {} seconds...\n", TEST_DURATION_SECS);

    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(TEST_DURATION_SECS) {
        match engine.recv_timeout(Duration::from_millis(200)) {
            Ok(metrics) => {
                let frames = metrics.frames_processed.max(1);

                let avg_encode = metrics.opus_encode_time_us / frames;

                let avg_decode = metrics.opus_decode_time_us / frames;

                println!(
                    "Frames: {} | Input errors: {} | \
                     Input dropped: {} | Output errors: {} | \
                     Output underruns: {} | Output dropped: {} | \
                     Opus bytes: {} | Encode avg: {} us | \
                     Decode avg: {} us",
                    metrics.frames_processed,
                    metrics.input_errors,
                    metrics.input_dropped,
                    metrics.output_errors,
                    metrics.output_underruns,
                    metrics.output_dropped,
                    metrics.opus_encoded_bytes,
                    avg_encode,
                    avg_decode,
                );
            }

            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                continue;
            }

            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                eprintln!("Audio worker disconnected.");
                break;
            }
        }
    }

    println!("\nStopping Audio Engine...");

    engine.stop().map_err(std::io::Error::other)?;

    println!("Audio Engine stopped.");

    Ok(())
}

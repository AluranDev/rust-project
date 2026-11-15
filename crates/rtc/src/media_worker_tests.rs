use crate::{
    audio_sender::{AudioSenderWorker, CaptureSource, capture_frame_samples},
    playback_worker::{PlaybackSink, PlaybackWorker},
    webrtc_connection::WebRtcConnection,
};
use audio::health::DeviceHealth;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, watch};
struct Source {
    health: DeviceHealth,
    dropped: Arc<AtomicBool>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl CaptureSource for Source {
    fn sample_rate(&self) -> u32 {
        11025
    }
    fn channels(&self) -> u16 {
        2
    }
    fn receive_into(&mut self, _: &mut [f32]) -> usize {
        0
    }
    fn check_health(&self) -> Result<(), String> {
        self.health.check()
    }
    fn take_backend_errors(&self) -> u64 {
        0
    }
    fn take_dropped_samples(&self) -> u64 {
        0
    }
}
struct Sink {
    health: DeviceHealth,
    dropped: Arc<AtomicBool>,
}
impl Drop for Sink {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
impl PlaybackSink for Sink {
    fn sample_rate(&self) -> u32 {
        11025
    }
    fn channels(&self) -> u16 {
        2
    }
    fn check_health(&self) -> Result<(), String> {
        self.health.check()
    }
    fn write(&mut self, samples: &[f32]) -> usize {
        samples.len()
    }
    fn take_backend_errors(&self) -> u64 {
        0
    }
    fn take_dropped_samples(&mut self) -> u64 {
        0
    }
    fn take_underrun_callbacks(&self) -> u64 {
        0
    }
}
async fn peer() -> Arc<WebRtcConnection> {
    let (signals, _rx) = mpsc::unbounded_channel();
    let (tracks, _rx) = mpsc::unbounded_channel();
    Arc::new(
        WebRtcConnection::new(1234, "a", "b", signals, tracks)
            .await
            .unwrap(),
    )
}
#[test]
fn stereo_fractional_frame_sizes_are_channel_aligned() {
    assert_eq!(capture_frame_samples(11025, 2, 20), 440);
    for rate in [8000, 11025, 22050, 44100, 48000, 96000] {
        for channels in [1, 2] {
            assert_eq!(
                capture_frame_samples(rate, channels, 20) % channels as usize,
                0
            );
        }
    }
}
#[tokio::test]
async fn input_failure_without_samples_stops_and_releases_capture() {
    let peer = peer().await;
    let health = DeviceHealth::default();
    let dropped = Arc::new(AtomicBool::new(false));
    let (_stop, stopped) = watch::channel(false);
    let worker = AudioSenderWorker::new(
        Source {
            health: health.clone(),
            dropped: dropped.clone(),
        },
        peer.clone(),
        20,
    )
    .unwrap();
    let fail = tokio::spawn(async move {
        tokio::task::yield_now().await;
        health.fail("microphone disconnected".into());
    });
    let error = tokio::time::timeout(Duration::from_millis(500), worker.run(stopped))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.contains("microphone disconnected"));
    assert!(dropped.load(Ordering::SeqCst));
    fail.await.unwrap();
    peer.close().await.unwrap();
}
#[tokio::test]
async fn playback_failure_without_frames_stops_and_releases_output() {
    let health = DeviceHealth::default();
    let dropped = Arc::new(AtomicBool::new(false));
    let (_tx, rx) = mpsc::channel(4);
    let worker = PlaybackWorker::new(
        Sink {
            health: health.clone(),
            dropped: dropped.clone(),
        },
        rx,
    );
    let fail = tokio::spawn(async move {
        tokio::task::yield_now().await;
        health.fail("output disconnected".into());
    });
    let error = tokio::time::timeout(Duration::from_millis(500), worker.run())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.contains("output disconnected"));
    assert!(dropped.load(Ordering::SeqCst));
    fail.await.unwrap();
}
#[tokio::test]
async fn cancellation_wakes_idle_input_and_output_without_aborting() {
    let peer = peer().await;
    let input_dropped = Arc::new(AtomicBool::new(false));
    let output_dropped = Arc::new(AtomicBool::new(false));
    let (stop, stopped) = watch::channel(false);
    let input = AudioSenderWorker::new(
        Source {
            health: DeviceHealth::default(),
            dropped: input_dropped.clone(),
        },
        peer.clone(),
        20,
    )
    .unwrap();
    let (_tx, rx) = mpsc::channel(4);
    let output = PlaybackWorker::new(
        Sink {
            health: DeviceHealth::default(),
            dropped: output_dropped.clone(),
        },
        rx,
    );
    let cancel = tokio::spawn(async move {
        tokio::task::yield_now().await;
        stop.send(true).unwrap();
    });
    tokio::time::timeout(Duration::from_millis(500), async {
        let (a, b) = tokio::join!(input.run(stopped.clone()), output.run_until(stopped));
        a.unwrap();
        b.unwrap();
    })
    .await
    .unwrap();
    assert!(input_dropped.load(Ordering::SeqCst) && output_dropped.load(Ordering::SeqCst));
    cancel.await.unwrap();
    peer.close().await.unwrap();
}

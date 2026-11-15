use cpal::traits::{DeviceTrait, HostTrait};

pub fn list_input_devices() -> Vec<cpal::Device> {
    let host = cpal::default_host();

    let devices: Vec<_> = host
        .input_devices()
        .map(|devices| devices.collect())
        .unwrap_or_default();

    println!("=== Audio Inputs ===");

    for (index, device) in devices.iter().enumerate() {
        let name = device
            .description()
            .map(|d| d.name().to_string())
            .unwrap_or_else(|_| "Unknown".into());

        println!("[{index}] {name}");
    }

    devices
}

/// Select an exact device name, or the operating system default when omitted.
pub fn select_input(name: Option<&str>) -> Result<cpal::Device, String> {
    let host = cpal::default_host();
    match name {
        None => host
            .default_input_device()
            .ok_or_else(|| "No default input device".into()),
        Some(name) => host
            .input_devices()
            .map_err(|e| e.to_string())?
            .find(|d| d.description().is_ok_and(|v| v.name() == name))
            .ok_or_else(|| format!("Input device not found: {name}")),
    }
}
pub fn select_output(name: Option<&str>) -> Result<cpal::Device, String> {
    let host = cpal::default_host();
    match name {
        None => host
            .default_output_device()
            .ok_or_else(|| "No default output device".into()),
        Some(name) => host
            .output_devices()
            .map_err(|e| e.to_string())?
            .find(|d| d.description().is_ok_and(|v| v.name() == name))
            .ok_or_else(|| format!("Output device not found: {name}")),
    }
}

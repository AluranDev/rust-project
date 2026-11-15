use std::sync::{Arc, Mutex};
/// Sticky backend failure, shared with the CPAL error callback.
#[derive(Clone, Default)]
pub struct DeviceHealth(Arc<Mutex<Option<String>>>);
impl DeviceHealth {
    pub fn fail(&self, error: String) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(error);
    }
    pub fn check(&self) -> Result<(), String> {
        match self.0.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

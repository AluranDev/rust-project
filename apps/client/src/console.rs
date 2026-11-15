//! A cancellable console reader. No stdin lock or blocking `lines()` survives a call.
use std::{
    collections::VecDeque,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::mpsc;
pub(crate) enum Poll {
    Data(Vec<u8>),
    Idle,
    Eof,
}
pub(crate) trait Source {
    fn poll(&mut self) -> io::Result<Poll>;
}
pub struct Console {
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    pub lines: mpsc::Receiver<Result<String, String>>,
}
impl Drop for Console {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl Console {
    pub fn start() -> io::Result<Self> {
        Self::with_source(Native::stdin)
    }
    pub(crate) fn with_source<F, S>(factory: F) -> io::Result<Self>
    where
        F: FnOnce() -> io::Result<S> + Send + 'static,
        S: Source + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let (tx, lines) = mpsc::channel(32);
        let handle = thread::Builder::new()
            .name("voice-console".into())
            .spawn(move || {
                let mut source = match factory() {
                    Ok(source) => source,
                    Err(error) => {
                        let _ = tx.try_send(Err(error.to_string()));
                        return;
                    }
                };
                let mut buffer = Vec::new();
                let mut pending = VecDeque::new();
                let mut eof = false;
                let mut too_long = false;
                while !stopped.load(Ordering::SeqCst) {
                    if let Some(line) = pending.pop_front() {
                        match tx.try_send(line) {
                            Ok(()) => (),
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                            Err(mpsc::error::TrySendError::Full(line)) => {
                                pending.push_front(line);
                                thread::sleep(Duration::from_millis(10));
                            }
                        }
                        continue;
                    }
                    if eof {
                        break;
                    }
                    match source.poll() {
                        Ok(Poll::Data(bytes)) => {
                            for byte in bytes {
                                match byte {
                                    b'\n' => {
                                        if !too_long {
                                            pending.push_back(
                                                String::from_utf8(std::mem::take(&mut buffer))
                                                    .map_err(|e| e.to_string()),
                                            );
                                        }
                                        buffer.clear();
                                        too_long = false;
                                    }
                                    b'\r' => (),
                                    8 | 127 => {
                                        buffer.pop();
                                    }
                                    _ if !too_long => {
                                        if buffer.len() < 4096 {
                                            buffer.push(byte);
                                        } else {
                                            too_long = true;
                                            buffer.clear();
                                            pending.push_back(Err(
                                                "console command exceeds 4096 bytes".into(),
                                            ));
                                        }
                                    }
                                    _ => (),
                                }
                            }
                        }
                        Ok(Poll::Idle) => thread::sleep(Duration::from_millis(10)),
                        Ok(Poll::Eof) => {
                            if !buffer.is_empty() {
                                pending.push_back(
                                    String::from_utf8(std::mem::take(&mut buffer))
                                        .map_err(|e| e.to_string()),
                                );
                            }
                            eof = true;
                        }
                        Err(error) => {
                            if !stopped.load(Ordering::SeqCst) {
                                pending.push_back(Err(error.to_string()));
                            }
                            eof = true;
                        }
                    }
                }
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
            lines,
        })
    }
    fn cancel(&self) {
        self.stop.store(true, Ordering::SeqCst);
        #[cfg(windows)]
        if let Some(handle) = &self.handle {
            use std::os::windows::io::AsRawHandle;
            // Also interrupt a synchronous redirected-file read, if one is pending.
            unsafe {
                windows_sys::Win32::System::IO::CancelSynchronousIo(handle.as_raw_handle());
            }
        }
    }
    pub fn request_shutdown(&self) {
        self.cancel();
    }
    pub async fn shutdown(mut self) -> Result<(), String> {
        self.cancel();
        let handle = self.handle.take().unwrap();
        tokio::task::spawn_blocking(move || {
            handle
                .join()
                .map_err(|_| "console reader panicked".to_owned())
        })
        .await
        .map_err(|e| e.to_string())?
    }
}
#[cfg(windows)]
struct Native {
    handle: windows_sys::Win32::Foundation::HANDLE,
    _owned: Option<std::os::windows::io::OwnedHandle>,
    console: bool,
    pipe: bool,
}
#[cfg(windows)]
impl Native {
    fn stdin() -> io::Result<Self> {
        unsafe {
            Self::from_handle(
                windows_sys::Win32::System::Console::GetStdHandle(
                    windows_sys::Win32::System::Console::STD_INPUT_HANDLE,
                ),
                None,
            )
        }
    }
    unsafe fn from_handle(
        handle: windows_sys::Win32::Foundation::HANDLE,
        owned: Option<std::os::windows::io::OwnedHandle>,
    ) -> io::Result<Self> {
        use windows_sys::Win32::{
            Foundation::INVALID_HANDLE_VALUE,
            Storage::FileSystem::{FILE_TYPE_PIPE, GetFileType},
            System::Console::GetConsoleMode,
        };
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "stdin is unavailable",
            ));
        }
        let mut mode = 0;
        let console = unsafe { GetConsoleMode(handle, &mut mode) } != 0;
        let pipe = unsafe { GetFileType(handle) } == FILE_TYPE_PIPE;
        Ok(Self {
            handle,
            _owned: owned,
            console,
            pipe,
        })
    }
}
#[cfg(windows)]
impl Source for Native {
    fn poll(&mut self) -> io::Result<Poll> {
        use std::io::Write;
        use windows_sys::Win32::{
            Foundation::{ERROR_BROKEN_PIPE, ERROR_HANDLE_EOF, ERROR_OPERATION_ABORTED},
            Storage::FileSystem::ReadFile,
            System::{Console::*, Pipes::PeekNamedPipe},
        };
        // Console input is consumed only after querying the available record count;
        // redirected pipes are read only when PeekNamedPipe reports available bytes.
        unsafe {
            if self.console {
                let mut available = 0;
                if GetNumberOfConsoleInputEvents(self.handle, &mut available) == 0 {
                    return Err(io::Error::last_os_error());
                }
                if available == 0 {
                    return Ok(Poll::Idle);
                }
                let mut record: INPUT_RECORD = std::mem::zeroed();
                let mut count = 0;
                if ReadConsoleInputW(self.handle, &mut record, 1, &mut count) == 0 {
                    return Err(io::Error::last_os_error());
                }
                if count == 0 || record.EventType != KEY_EVENT as u16 {
                    return Ok(Poll::Data(Vec::new()));
                }
                let key = record.Event.KeyEvent;
                if key.bKeyDown == 0 {
                    return Ok(Poll::Data(Vec::new()));
                }
                let code = key.uChar.UnicodeChar;
                let mut bytes = Vec::new();
                for _ in 0..key.wRepeatCount {
                    match code {
                        13 => {
                            println!();
                            bytes.push(b'\n');
                        }
                        8 => {
                            print!("\x08 \x08");
                            bytes.push(8);
                        }
                        32..=126 => {
                            print!("{}", code as u8 as char);
                            bytes.push(code as u8);
                        }
                        _ => (),
                    }
                }
                let _ = io::stdout().flush();
                return Ok(Poll::Data(bytes));
            }
            let mut available = 512;
            if self.pipe {
                if PeekNamedPipe(
                    self.handle,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    &mut available,
                    std::ptr::null_mut(),
                ) == 0
                {
                    let error = io::Error::last_os_error();
                    if error.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) {
                        return Ok(Poll::Eof);
                    }
                    return Err(error);
                }
                if available == 0 {
                    return Ok(Poll::Idle);
                }
            }
            let mut bytes = vec![0; available.min(512) as usize];
            let mut count = 0;
            if ReadFile(
                self.handle,
                bytes.as_mut_ptr(),
                bytes.len() as u32,
                &mut count,
                std::ptr::null_mut(),
            ) == 0
            {
                let error = io::Error::last_os_error();
                if matches!(error.raw_os_error(),Some(value) if value==ERROR_BROKEN_PIPE as i32||value==ERROR_HANDLE_EOF as i32||value==ERROR_OPERATION_ABORTED as i32)
                {
                    return Ok(Poll::Eof);
                }
                return Err(error);
            }
            if count == 0 {
                return Ok(Poll::Eof);
            }
            bytes.truncate(count as usize);
            Ok(Poll::Data(bytes))
        }
    }
}
#[cfg(unix)]
struct Native;
#[cfg(unix)]
impl Native {
    fn stdin() -> io::Result<Self> {
        Ok(Self)
    }
}
#[cfg(unix)]
impl Source for Native {
    fn poll(&mut self) -> io::Result<Poll> {
        unsafe {
            let mut fd = libc::pollfd {
                fd: libc::STDIN_FILENO,
                events: libc::POLLIN,
                revents: 0,
            };
            let ready = libc::poll(&mut fd, 1, 0);
            if ready < 0 {
                return Err(io::Error::last_os_error());
            }
            if ready == 0 {
                return Ok(Poll::Idle);
            }
            let mut bytes = vec![0; 512];
            let count = libc::read(libc::STDIN_FILENO, bytes.as_mut_ptr().cast(), bytes.len());
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 {
                return Ok(Poll::Eof);
            }
            bytes.truncate(count as usize);
            Ok(Poll::Data(bytes))
        }
    }
}
#[cfg(not(any(windows, unix)))]
struct Native;
#[cfg(not(any(windows, unix)))]
impl Native {
    fn stdin() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "interactive input unsupported",
        ))
    }
}
#[cfg(not(any(windows, unix)))]
impl Source for Native {
    fn poll(&mut self) -> io::Result<Poll> {
        Ok(Poll::Eof)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Idle(Arc<AtomicBool>);
    impl Drop for Idle {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    impl Source for Idle {
        fn poll(&mut self) -> io::Result<Poll> {
            Ok(Poll::Idle)
        }
    }
    #[tokio::test]
    async fn idle_reader_can_be_closed_and_reopened_without_input() {
        for _ in 0..3 {
            let dropped = Arc::new(AtomicBool::new(false));
            let flag = dropped.clone();
            let reader = Console::with_source(move || Ok(Idle(flag))).unwrap();
            tokio::time::timeout(Duration::from_secs(1), reader.shutdown())
                .await
                .unwrap()
                .unwrap();
            assert!(dropped.load(Ordering::SeqCst));
        }
    }
    #[cfg(windows)]
    #[tokio::test]
    async fn real_pipe_reader_stops_with_writer_still_open() {
        use std::{
            io::Write,
            os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        };
        use windows_sys::Win32::System::Pipes::CreatePipe;
        let mut read = std::ptr::null_mut();
        let mut write = std::ptr::null_mut();
        assert_ne!(
            unsafe { CreatePipe(&mut read, &mut write, std::ptr::null(), 0) },
            0
        );
        let read = unsafe { OwnedHandle::from_raw_handle(read) };
        let write = unsafe { OwnedHandle::from_raw_handle(write) };
        let mut writer = std::fs::File::from(write);
        let mut reader = Console::with_source(move || unsafe {
            Native::from_handle(read.as_raw_handle(), Some(read))
        })
        .unwrap();
        writer.write_all(b"devices\n").unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), reader.lines.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            "devices"
        );
        tokio::time::timeout(Duration::from_secs(1), reader.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(writer.write_all(b"quit\n").is_err());
    }
}

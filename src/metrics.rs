use crate::ThreadPool;
use byte_unit::Byte;
use chrono::{DateTime, Local};
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;
use std::sync::mpsc::{channel, RecvError, Sender, TryRecvError};
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub enum Metric {
    End,
    BytesProcessed(u64),
    ChunkProcessed,
    FileProcessed,
}

pub struct Collector {
    sender: Sender<Metric>,
    t: Option<JoinHandle<()>>,
}

impl Collector {
    pub fn new() -> Self {
        let (sender, receiver) = channel::<Metric>();
        Self {
            sender,
            t: thread::Builder::new()
                .name("stats-collector".to_string())
                .spawn(move || {
                    let mut running = true;
                    let mut metrics = Metrics::new();
                    let mut timestamp = Self::timestamp();

                    loop {
                        let elapsed = loop {
                            let elapsed = Self::timestamp() - timestamp;

                            if elapsed > 1000 {
                                break elapsed;
                            }
                            match receiver.try_recv() {
                                Ok(Metric::BytesProcessed(bytes)) => {
                                    metrics.bytes_processed += bytes
                                }
                                Ok(Metric::FileProcessed) => metrics.files_processed += 1,
                                Ok(Metric::ChunkProcessed) => metrics.chunks_processed += 1,
                                Ok(Metric::End) => {
                                    log::debug!("end");
                                    running = false;
                                    break elapsed;
                                }
                                Err(TryRecvError::Empty) => {
                                    thread::sleep(Duration::from_millis(500))
                                }
                                Err(_) => break elapsed,
                            }
                        };

                        if metrics.bytes_processed > 0 {
                            metrics.bytes_processed_total += metrics.bytes_processed;
                            metrics.process_speed = metrics.bytes_processed as f64 / elapsed as f64;
                        }
                        log::info!(
                            "{} - {}",
                            DateTime::<Local>::from(SystemTime::now()).format("%T"),
                            metrics
                        );
                        metrics.reset();
                        timestamp = Self::timestamp();

                        if !running {
                            break;
                        }
                    }
                })
                .ok(),
        }
    }

    pub fn sender(&self) -> Sender<Metric> {
        self.sender.clone()
    }

    fn timestamp() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis()
    }

    pub fn stop(&self) {
        let _ = self.sender.send(Metric::End);
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        log::debug!("drop");
        let _ = self.sender.send(Metric::End);
        if let Some(t) = self.t.take() {
            let _ = t.join();
        }
    }
}

struct Metrics {
    start_timestamp: u128,
    chunks_processed: u64,
    files_processed: u64,
    bytes_processed: u64,
    process_speed: f64,
    bytes_processed_total: u64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            start_timestamp: Collector::timestamp(),
            chunks_processed: 0,
            files_processed: 0,
            bytes_processed: 0,
            process_speed: 0.0,
            bytes_processed_total: 0,
        }
    }

    fn reset(&mut self) {
        self.bytes_processed = 0;
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} chunks processed in {} files; {}/sec ({}/sec avg)",
            self.chunks_processed,
            self.files_processed,
            Byte::from_bytes((self.process_speed) as u128).get_appropriate_unit(false),
            Byte::from_bytes(
                (self.bytes_processed_total as f64
                    / (Collector::timestamp() - self.start_timestamp) as f64)
                    as u128
            )
            .get_appropriate_unit(false)
        )
    }
}

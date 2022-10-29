use byte_unit::Byte;
use chrono_humanize::{Accuracy, HumanTime, Tense};
use std::fmt::{Display, Formatter};
use std::sync::mpsc::{channel, Sender, TryRecvError};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const POINT_FREQ_MS: u128 = 1000;
const SLEEP_MS: u128 = (POINT_FREQ_MS as f64 / 2f64) as u128;

pub enum Metric {
    End,
    BytesTransferred(u64),
    ChunkProcessed,
    FileProcessed,
    ChunksTotal(u64),
    FilesTotal(u64),
    BytesTotal(u64),
}

pub struct Collector {
    sender: Sender<Metric>,
    t: Option<JoinHandle<()>>,
}

impl Collector {}

impl Collector {
    pub fn new() -> Self {
        let (sender, receiver) = channel::<Metric>();
        Self {
            sender,
            t: thread::Builder::new()
                .name("stats-collector".to_string())
                .spawn(move || {
                    let start_timestamp = Self::timestamp();

                    let mut running = true;
                    let mut metrics = Metrics::new();
                    let mut timestamp = start_timestamp;

                    loop {
                        let elapsed = loop {
                            let elapsed = Self::timestamp() - timestamp;

                            if elapsed > POINT_FREQ_MS {
                                break elapsed;
                            }
                            match receiver.try_recv() {
                                Ok(Metric::BytesTransferred(bytes)) => {
                                    metrics.inc_bytes_transferred_count(bytes)
                                }
                                Ok(Metric::FileProcessed) => metrics.inc_files_processed(),
                                Ok(Metric::ChunkProcessed) => metrics.inc_chunks_processed(),
                                Ok(Metric::ChunksTotal(count)) => metrics.set_chunks_total(count),
                                Ok(Metric::FilesTotal(count)) => metrics.set_files_total(count),
                                Ok(Metric::BytesTotal(bytes)) => metrics.set_bytes_total(bytes),
                                Ok(Metric::End) => {
                                    log::debug!("end");
                                    running = false;
                                    break 0;
                                }
                                Err(TryRecvError::Empty) => {
                                    thread::sleep(Duration::from_millis(SLEEP_MS as u64))
                                }
                                Err(_) => break elapsed,
                            }
                        };

                        if elapsed > SLEEP_MS {
                            timestamp = Self::timestamp();
                            log::info!("{}", metrics.point(elapsed, timestamp - start_timestamp));
                        }

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
    chunks_total: u64,
    files_total: u64,
    bytes_total: u64,
    chunks_transferred: u64,
    files_transferred: u64,
    bytes_transferred: u64,
    bytes_transferred_prev: u64,
    bytes_transferred_total: u64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            chunks_total: 0,
            files_total: 0,
            bytes_total: 0,
            chunks_transferred: 0,
            files_transferred: 0,
            bytes_transferred: 0,
            bytes_transferred_prev: 0,
            bytes_transferred_total: 0,
        }
    }

    fn point(&mut self, elapsed: u128, total_elapsed: u128) -> Point {
        let elapsed = elapsed as f64 / 1000f64;
        let total_elapsed = total_elapsed as f64 / 1000f64;
        let p = Point {
            chunks_total: self.chunks_total,
            files_total: self.files_total,
            bytes_total: self.bytes_total,

            chunks_transferred: self.chunks_transferred,
            files_transferred: self.files_transferred,
            bytes_transferred: self.bytes_transferred,

            transfer_rate: if self.bytes_transferred > 0 {
                self.bytes_transferred as f64 / elapsed as f64
            } else {
                self.bytes_transferred_prev as f64 / elapsed as f64
            },
            avg_transfer_rate: self.bytes_transferred_total as f64 / total_elapsed,
        };
        if self.bytes_transferred > 0 {
            self.bytes_transferred_prev = self.bytes_transferred;
            self.bytes_transferred = 0;
        }
        p
    }

    fn inc_files_processed(&mut self) {
        self.files_transferred += 1;
    }

    fn inc_chunks_processed(&mut self) {
        self.chunks_transferred += 1;
    }

    fn inc_bytes_transferred_count(&mut self, bytes: u64) {
        self.bytes_transferred += bytes;
        self.bytes_transferred_total += bytes;
    }

    fn set_chunks_total(&mut self, count: u64) {
        self.chunks_total = count;
    }

    fn set_files_total(&mut self, count: u64) {
        self.files_total = count;
    }

    fn set_bytes_total(&mut self, count: u64) {
        self.bytes_total = count;
    }
}

struct Point {
    chunks_total: u64,
    files_total: u64,
    bytes_total: u64,
    chunks_transferred: u64,
    files_transferred: u64,
    bytes_transferred: u64,
    transfer_rate: f64,
    avg_transfer_rate: f64,
}

impl Display for Point {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let eta = if self.avg_transfer_rate > 0f64 {
            let remaining_bytes = (self.bytes_total - self.bytes_transferred) as f64;
            HumanTime::from(chrono::Duration::seconds(
                (remaining_bytes / self.avg_transfer_rate as f64) as i64,
            ))
            .to_text_en(Accuracy::Rough, Tense::Future)
        } else {
            "?".to_string()
        };
        let percent = if self.bytes_total > 0 {
            self.bytes_transferred as f64 / self.bytes_total as f64 * 100f64
        } else {
            0f64
        };
        write!(
            f,
            "{percent:.2}%, ETA {eta} - {p_chunks}/{t_chunks} chunks; {p_files}/{t_files} files; {p_bytes}/{t_bytes} bytes; rate: {rate}/sec ({avg_rate}/sec avg)",
            percent = percent,
            eta = eta,
            p_chunks = self.chunks_transferred,
            t_chunks = self.chunks_total,
            p_files = self.files_transferred,
            t_files = self.files_total,
            p_bytes = Byte::from_bytes(self.bytes_transferred as u128).get_appropriate_unit(false),
            t_bytes = Byte::from_bytes(self.bytes_total as u128).get_appropriate_unit(false),
            rate = Byte::from_bytes(self.transfer_rate as u128).get_appropriate_unit(false),
            avg_rate = Byte::from_bytes(self.avg_transfer_rate as u128).get_appropriate_unit(false),
        )
    }
}

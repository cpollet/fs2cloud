use anyhow::{bail, Result};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

pub struct ThreadPool {
    workers: usize,
    worker_threads: Vec<Worker>,
    sender: mpsc::SyncSender<Message>,
    callback: Option<Box<dyn FnMut()>>,
}

struct Worker {
    t: Option<JoinHandle<()>>,
}

type Job = Box<dyn FnOnce() + 'static + Send>;
enum Message {
    End,
    Job(Job),
}

impl ThreadPool {
    pub fn new(workers: usize, max_queue_size: usize) -> Self {
        log::debug!("init with {} {}", workers, max_queue_size);

        let (sender, receiver) = mpsc::sync_channel(max_queue_size);
        let receiver = Arc::new(Mutex::new(receiver));

        let mut worker_threads = Vec::with_capacity(workers);
        for i in 0..workers {
            worker_threads.push(Worker::new(i, receiver.clone()))
        }

        Self {
            workers,
            worker_threads,
            sender,
            callback: None,
        }
    }

    pub fn with_callback(mut self, callback: Box<dyn FnMut()>) -> Self {
        self.callback = Some(callback);
        self
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    pub fn execute<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce() + 'static + Send,
    {
        let job = Message::Job(Box::new(f));
        match self.sender.send(job) {
            Ok(_) => Ok(()),
            Err(e) => {
                bail!("Unable to start job: {:#}", e)
            }
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        log::debug!("drop");
        for _ in 0..self.workers {
            self.sender.send(Message::End).unwrap();
        }
        self.worker_threads.iter_mut().for_each(|w| {
            if let Some(t) = w.t.take() {
                t.join().unwrap();
            }
        });
        if let Some(callback) = &mut self.callback {
            callback();
        }
    }
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        log::debug!("worker[{}] ready", id);
        let t = thread::Builder::new()
            .name(format!("worker-{}", id))
            .spawn(move || {
                loop {
                    let message = { receiver.lock().unwrap().recv().unwrap() };
                    match message {
                        Message::End => {
                            log::debug!("worker[{}] end", id);
                            break;
                        }
                        Message::Job(job) => {
                            log::debug!("worker[{}] start", id);
                            job();
                            log::debug!("worker[{}] finish", id);
                        }
                    }
                }
                log::debug!("worker[{}] done", id)
            })
            .unwrap();
        Self { t: Some(t) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;
    use std::time::Duration;

    #[test]
    fn it_works() {
        let p = ThreadPool::new(4, 2);
        println!("job1:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job1:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job1: done");
        });
        println!("job2:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job2:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job2: done");
        });
        println!("job3:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job3:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job3: done");
        });
        println!("job4:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job4:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job4: done");
        });
        println!("job5:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job5:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job5: done");
        });
        println!("job6:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job6:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job6: done");
        });
        println!("job7:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job7:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job7: done");
        });
        println!("job8:queue");
        let _ = p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job8:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job8: done");
        });
    }
}

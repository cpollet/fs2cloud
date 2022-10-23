use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

pub struct ThreadPool {
    max_workers: usize,
    workers: Vec<Worker>,
    sender: mpsc::SyncSender<Message>,
}

pub trait ThreadPoolConfig {
    fn get_max_workers_count(&self) -> usize;
    fn get_max_queue_size(&self) -> usize;
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
    pub fn new(config: &dyn ThreadPoolConfig) -> Self {
        let max_workers = config.get_max_workers_count();
        log::debug!("init with {} {}", max_workers, config.get_max_queue_size());

        let (sender, receiver) = mpsc::sync_channel(config.get_max_queue_size());
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(max_workers);
        for i in 0..max_workers {
            workers.push(Worker::new(i, receiver.clone()))
        }

        Self {
            max_workers,
            workers,
            sender,
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + 'static + Send,
    {
        let job = Message::Job(Box::new(f));
        self.sender.send(job).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        log::debug!("drop");
        for _ in 0..self.max_workers {
            self.sender.send(Message::End).unwrap();
        }
        self.workers.iter_mut().for_each(|w| {
            if let Some(t) = w.t.take() {
                t.join().unwrap();
            }
        });
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

    struct Config {}
    impl ThreadPoolConfig for Config {
        fn get_max_workers_count(&self) -> usize {
            4
        }

        fn get_max_queue_size(&self) -> usize {
            2
        }
    }

    #[test]
    fn it_works() {
        let config = Config {};
        let p = ThreadPool::new(&config);
        println!("job1:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job1:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job1: done");
        });
        println!("job2:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job2:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job2: done");
        });
        println!("job3:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job3:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job3: done");
        });
        println!("job4:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job4:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job4: done");
        });
        println!("job5:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job5:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job5: done");
        });
        println!("job6:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job6:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job6: done");
        });
        println!("job7:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job7:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job7: done");
        });
        println!("job8:queue");
        p.execute(|| {
            let mut rand = rand::thread_rng();
            let sleep = Duration::from_millis(10000u64 + (1000f32 * rand.gen::<f32>()) as u64);
            println!("job8:start:{}", sleep.as_millis());
            thread::sleep(sleep);
            println!("job8: done");
        });
    }
}

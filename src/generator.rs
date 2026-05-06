use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::task::Task;

pub struct WorkloadConfig {
    pub num_tasks:           u64,
    pub seed:                u64,
    pub io_fraction:         f64,  // fraction of tasks that are IO
    pub arrival_interval_ms: u64,  // fixed gap between consecutive task arrivals
}

impl WorkloadConfig {
    /// 70% IO / 30% CPU, one task every 20 ms.
    pub fn standard(seed: u64) -> Self {
        Self { num_tasks: 1000, seed, io_fraction: 0.70, arrival_interval_ms: 20 }
    }

    /// 80% IO / 20% CPU variant.
    pub fn heavy_io(seed: u64) -> Self {
        Self { num_tasks: 1000, seed: seed + 1, io_fraction: 0.80, arrival_interval_ms: 20 }
    }
}

/// Sends tasks one at a time at fixed intervals, then drops the sender.
pub fn run(tx: Sender<Task>, config: WorkloadConfig) {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let interval = Duration::from_millis(config.arrival_interval_ms);

    for id in 0..config.num_tasks {
        thread::sleep(interval);
        let task = if rng.gen::<f64>() < config.io_fraction {
            Task::new_io(id)
        } else {
            Task::new_cpu(id)
        };
        if tx.send(task).is_err() {
            break;
        }
    }
}

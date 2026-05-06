use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::task::{Task, TaskKind};

pub struct WorkloadConfig {
    pub num_tasks: u64,
    pub seed: u64,
    pub cpu_fraction: f64,
    pub burst_mode: bool,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    pub arrival_spread_ms: u64,
}

impl WorkloadConfig {
    /// Balanced: ~50% CPU, ~50% IO, tasks spread evenly over 2 seconds.
    pub fn balanced(num_tasks: u64, seed: u64) -> Self {
        Self {
            num_tasks,
            seed,
            cpu_fraction: 0.5,
            burst_mode: false,
            min_duration_ms: 5,
            max_duration_ms: 20,
            arrival_spread_ms: 2000,
        }
    }

    /// Stressed: 85% CPU, burst arrivals (70% arrive in the first 20% of the window).
    pub fn stressed(num_tasks: u64, seed: u64) -> Self {
        Self {
            num_tasks,
            seed: seed + 1,
            cpu_fraction: 0.85,
            burst_mode: true,
            min_duration_ms: 10,
            max_duration_ms: 60,
            arrival_spread_ms: 400,
        }
    }
}

/// Runs in its own thread. Sends tasks in arrival-time order, then drops the sender
/// to signal the dispatcher that generation is complete.
pub fn run(tx: Sender<Task>, config: WorkloadConfig) {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let gen_start = Instant::now();

    // Pre-generate all arrival offsets and sort so we release tasks in order.
    let mut offsets: Vec<u64> = (0..config.num_tasks)
        .map(|_| {
            if config.burst_mode {
                let burst_end = config.arrival_spread_ms / 5;
                if rng.gen::<f64>() < 0.7 {
                    rng.gen_range(0..burst_end)
                } else {
                    rng.gen_range(0..config.arrival_spread_ms)
                }
            } else {
                rng.gen_range(0..config.arrival_spread_ms)
            }
        })
        .collect();
    offsets.sort_unstable();

    for (i, offset_ms) in offsets.into_iter().enumerate() {
        let target = gen_start + Duration::from_millis(offset_ms);
        let now = Instant::now();
        if target > now {
            thread::sleep(target - now);
        }

        let kind = if rng.gen::<f64>() < config.cpu_fraction {
            TaskKind::Cpu
        } else {
            TaskKind::Io
        };
        let duration_ms = rng.gen_range(config.min_duration_ms..=config.max_duration_ms);
        let priority = rng.gen_range(1_i32..=10_i32);

        let task = Task {
            id: i as u64,
            arrival_time: Instant::now(),
            kind,
            duration_ms,
            priority,
        };

        if tx.send(task).is_err() {
            break; // dispatcher disconnected — stop early
        }
    }
    // Dropping tx here signals the dispatcher that all tasks have been generated.
}

mod dispatcher;
mod generator;
mod metrics;
mod task;
mod worker;

use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;

use generator::WorkloadConfig;
use metrics::Metrics;

const NUM_WORKERS: usize = 8;
const NUM_TASKS: u64 = 600;
const SEED: u64 = 42;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let experiment = args.get(1).map(|s| s.as_str()).unwrap_or("balanced");

    let config = match experiment {
        "stressed" => {
            println!("Experiment : Stressed workload (85% CPU, burst arrivals)");
            WorkloadConfig::stressed(NUM_TASKS, SEED)
        }
        _ => {
            println!("Experiment : Balanced workload (50/50 CPU-IO, uniform arrivals)");
            WorkloadConfig::balanced(NUM_TASKS, SEED)
        }
    };

    println!("Workers    : {NUM_WORKERS}");
    println!("Tasks      : {NUM_TASKS}");
    println!("Policy     : priority-based with aging");
    println!();

    // Metrics is written by the dispatcher and read by main after all threads join.
    let metrics = Arc::new(Mutex::new(Metrics::new(NUM_WORKERS)));
    let metrics_d = Arc::clone(&metrics);

    // Channel: generator → dispatcher
    let (gen_tx, gen_rx) = mpsc::channel::<task::Task>();

    // Channel: workers → dispatcher (one shared sender, cloned per worker)
    let (comp_tx, comp_rx) = mpsc::channel::<metrics::CompletionReport>();

    // Per-worker channels: dispatcher → worker i
    let mut worker_txs = Vec::with_capacity(NUM_WORKERS);
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);

    for id in 0..NUM_WORKERS {
        let (wtx, wrx) = mpsc::channel::<Option<task::Task>>();
        worker_txs.push(wtx);
        let ctxc = comp_tx.clone();
        worker_handles.push(thread::spawn(move || worker::run(id, wrx, ctxc)));
    }
    // The original comp_tx must be dropped so the channel closes when all workers exit.
    drop(comp_tx);

    // Dispatcher thread: owns both queues, drives the scheduling loop.
    let dispatch_handle = thread::spawn(move || {
        dispatcher::run(gen_rx, comp_rx, worker_txs, metrics_d);
    });

    // Generator thread: sends tasks in arrival-time order, then drops its sender.
    let gen_handle = thread::spawn(move || generator::run(gen_tx, config));

    // Join in natural order: generator first (it drives everything else).
    gen_handle.join().expect("generator panicked");
    dispatch_handle.join().expect("dispatcher panicked");
    for h in worker_handles {
        h.join().expect("worker panicked");
    }

    println!("=== Results ===");
    metrics.lock().unwrap().print_summary();
}

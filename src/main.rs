mod dispatcher;
mod generator;
mod metrics;
mod task;
mod worker;

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dispatcher::Policy;
use generator::WorkloadConfig;
use metrics::{Metrics, MonitorSample};

const NUM_WORKERS: usize = 8;
const SEED: u64 = 42;
const MONITOR_INTERVAL_MS: u64 = 10;

/// Runs one complete simulation (all threads) and returns the filled Metrics.
fn run_simulation(config: WorkloadConfig, policy: Policy, label: &str) -> Metrics {
    println!("Running {label}...");

    let global_cpu     = Arc::new(AtomicU32::new(0));
    let active_workers = Arc::new(AtomicUsize::new(0));
    let stop_monitor   = Arc::new(AtomicBool::new(false));

    let metrics = Arc::new(Mutex::new(Metrics::new()));

    // Channel: generator → manager
    let (gen_tx, gen_rx) = mpsc::channel::<task::Task>();

    // Channel: workers → manager (completions)
    let (comp_tx, comp_rx) = mpsc::channel::<metrics::CompletionReport>();

    // Per-worker channels: manager → worker
    let mut worker_txs     = Vec::with_capacity(NUM_WORKERS);
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);

    for id in 0..NUM_WORKERS {
        let (wtx, wrx) = mpsc::channel::<Option<task::Task>>();
        worker_txs.push(wtx);
        let ctxc = comp_tx.clone();
        worker_handles.push(thread::spawn(move || worker::run(id, wrx, ctxc)));
    }
    drop(comp_tx);

    // Manager queue thread
    let metrics_d       = Arc::clone(&metrics);
    let global_cpu_d    = Arc::clone(&global_cpu);
    let active_workers_d = Arc::clone(&active_workers);
    let manager_handle  = thread::spawn(move || {
        dispatcher::run(gen_rx, comp_rx, worker_txs, metrics_d,
                        global_cpu_d, active_workers_d, policy);
    });

    // Generator thread
    let gen_handle = thread::spawn(move || generator::run(gen_tx, config));

    // Monitor thread: samples global CPU% and active worker count every 10 ms
    let global_cpu_m    = Arc::clone(&global_cpu);
    let active_workers_m = Arc::clone(&active_workers);
    let metrics_m       = Arc::clone(&metrics);
    let stop_m          = Arc::clone(&stop_monitor);
    let monitor_handle  = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(MONITOR_INTERVAL_MS));
            if stop_m.load(Ordering::Relaxed) { break; }
            let sample = MonitorSample {
                cpu_percent:    global_cpu_m.load(Ordering::Relaxed),
                active_workers: active_workers_m.load(Ordering::Relaxed),
            };
            metrics_m.lock().unwrap().add_sample(sample);
        }
    });

    gen_handle.join().expect("generator panicked");
    manager_handle.join().expect("manager panicked");
    for h in worker_handles {
        h.join().expect("worker panicked");
    }

    stop_monitor.store(true, Ordering::Relaxed);
    monitor_handle.join().ok();

    Arc::try_unwrap(metrics)
        .unwrap_or_else(|_| panic!("metrics still borrowed"))
        .into_inner()
        .unwrap()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let workload_arg = args.get(1).map(|s| s.as_str()).unwrap_or("70-30");

    let (config_a, config_b, workload_label) = match workload_arg {
        "80-20" => (
            WorkloadConfig::heavy_io(SEED),
            WorkloadConfig::heavy_io(SEED),
            "80% IO / 20% CPU",
        ),
        _ => (
            WorkloadConfig::standard(SEED),
            WorkloadConfig::standard(SEED),
            "70% IO / 30% CPU",
        ),
    };

    println!("╔══════════════════════════════════════════════╗");
    println!("║       Concurrent Task Dispatcher             ║");
    println!("╚══════════════════════════════════════════════╝");
    println!("Workload   : {workload_label}  |  1000 tasks, 20 ms intervals");
    println!("Workers    : {NUM_WORKERS}");
    println!("Task times : CPU = 35% load, IO = 10% load, both run 200 ms");
    println!("CPU cap    : 100%  (manager blocks dispatch if cap would be exceeded)");
    println!();

    let result_fifo = run_simulation(config_a, Policy::Fifo,      "Simulation 1 — FIFO");
    println!();
    let result_opt  = run_simulation(config_b, Policy::Optimized, "Simulation 2 — Optimized");
    println!();

    result_fifo.print_summary("Simulation 1 — FIFO");
    println!();
    result_opt.print_summary("Simulation 2 — Optimized");
    println!();

    // Comparison
    let rt_fifo = result_fifo.total_runtime_ms();
    let rt_opt  = result_opt.total_runtime_ms();
    let speedup = rt_fifo as f64 / rt_opt as f64;
    println!("=== Comparison ===");
    println!("Runtime    : FIFO {rt_fifo} ms  vs  Optimized {rt_opt} ms  ({speedup:.2}x speedup)");
    println!("Avg CPU    : FIFO {}%  vs  Optimized {}%",
             result_fifo.avg_cpu(), result_opt.avg_cpu());
}

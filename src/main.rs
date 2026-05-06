mod dispatcher;
mod generator;
mod metrics;
mod task;
mod worker;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use dispatcher::DispatchConfig;
use generator::WorkloadConfig;
use metrics::Metrics;

const NUM_WORKERS: usize = 8;
const NUM_TASKS: u64 = 600;
const SEED: u64 = 42;

/// Number of workers reserved exclusively for CPU tasks in the "reserved" policy.
const RESERVED_CPU_WORKERS: usize = 2;

fn print_usage() {
    eprintln!("Usage: task-dispatcher [balanced|stressed] [priority|reserved]");
    eprintln!("  balanced  — Experiment A: 50/50 CPU-IO, uniform arrivals (default)");
    eprintln!("  stressed  — Experiment B: 85% CPU, burst arrivals");
    eprintln!("  priority  — Scheduling policy: priority + aging (default)");
    eprintln!("  reserved  — Scheduling policy: priority + aging + 2 CPU-reserved workers");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_usage();
        return;
    }

    let workload = args.get(1).map(|s| s.as_str()).unwrap_or("balanced");
    let policy   = args.get(2).map(|s| s.as_str()).unwrap_or("priority");

    let (workload_config, workload_label) = match workload {
        "stressed" => (
            WorkloadConfig::stressed(NUM_TASKS, SEED),
            "Experiment B — Stressed (85% CPU, burst arrivals)",
        ),
        _ => (
            WorkloadConfig::balanced(NUM_TASKS, SEED),
            "Experiment A — Balanced (50/50 CPU-IO, uniform arrivals)",
        ),
    };

    let (dispatch_config, policy_label) = match policy {
        "reserved" => (
            DispatchConfig { reserved_cpu_workers: RESERVED_CPU_WORKERS },
            format!("priority + aging  +  {RESERVED_CPU_WORKERS} CPU-reserved workers"),
        ),
        _ => (
            DispatchConfig { reserved_cpu_workers: 0 },
            "priority + aging (no reservation)".to_string(),
        ),
    };

    println!("╔══════════════════════════════════════════════╗");
    println!("║       Concurrent Task Dispatcher             ║");
    println!("╚══════════════════════════════════════════════╝");
    println!("Workload   : {workload_label}");
    println!("Policy     : {policy_label}");
    println!("Workers    : {NUM_WORKERS}  ({} CPU-reserved, {} general)",
             dispatch_config.reserved_cpu_workers,
             NUM_WORKERS - dispatch_config.reserved_cpu_workers);
    println!("Tasks      : {NUM_TASKS}");
    println!("Seed       : {SEED}");
    println!();

    // Live completion counter shared between the dispatcher (writer)
    // and the monitor thread (reader).
    let counter = Arc::new(AtomicUsize::new(0));

    // Stop flag for the monitor thread.
    let stop_monitor = Arc::new(AtomicBool::new(false));

    // Metrics: written by dispatcher, read by main after all threads join.
    let metrics = Arc::new(Mutex::new(Metrics::new(NUM_WORKERS, Arc::clone(&counter))));

    // Channel: generator → dispatcher
    let (gen_tx, gen_rx) = mpsc::channel::<task::Task>();

    // Channel: workers → dispatcher (one sender per worker, all share one receiver)
    let (comp_tx, comp_rx) = mpsc::channel::<metrics::CompletionReport>();

    // Per-worker channels: dispatcher → worker i
    let mut worker_txs     = Vec::with_capacity(NUM_WORKERS);
    let mut worker_handles = Vec::with_capacity(NUM_WORKERS);

    for id in 0..NUM_WORKERS {
        let (wtx, wrx) = mpsc::channel::<Option<task::Task>>();
        worker_txs.push(wtx);
        let ctxc = comp_tx.clone();
        worker_handles.push(thread::spawn(move || worker::run(id, wrx, ctxc)));
    }
    // Drop the original so the channel closes when all worker clones are dropped.
    drop(comp_tx);

    // Dispatcher thread: owns both queues, drives the scheduling loop.
    let metrics_d = Arc::clone(&metrics);
    let dispatch_handle = thread::spawn(move || {
        dispatcher::run(gen_rx, comp_rx, worker_txs, metrics_d, dispatch_config);
    });

    // Generator thread: sends tasks in arrival-time order, then drops its sender.
    let gen_handle = thread::spawn(move || generator::run(gen_tx, workload_config));

    // Monitor thread: prints live progress every 500 ms.
    let counter_m    = Arc::clone(&counter);
    let stop_m       = Arc::clone(&stop_monitor);
    let monitor_handle = thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(500));
            if stop_m.load(Ordering::Relaxed) { break; }
            let done = counter_m.load(Ordering::Relaxed);
            println!("[monitor] {done:>3}/{NUM_TASKS} tasks completed");
        }
    });

    // Join in natural order: generator finishes first (drives everything else).
    gen_handle.join().expect("generator panicked");
    dispatch_handle.join().expect("dispatcher panicked");
    for h in worker_handles {
        h.join().expect("worker panicked");
    }

    // Signal and join the monitor now that all work is done.
    stop_monitor.store(true, Ordering::Relaxed);
    monitor_handle.join().ok();

    println!();
    metrics.lock().unwrap().print_summary();
}

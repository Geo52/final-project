use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use crate::metrics::{CompletionReport, Metrics};
use crate::task::{Task, TaskKind};

#[derive(Clone, Copy)]
pub enum Policy {
    /// Dispatch tasks in strict arrival order. If the front task would push
    /// global CPU over 100%, the manager waits — even if IO tasks behind it
    /// would fit. This is head-of-line blocking.
    Fifo,
    /// Look at what's in both queues and pick the task type that best fills
    /// the remaining CPU headroom without exceeding 100%. Avoids blocking.
    Optimized,
}

/// Runs in its own thread (the "manager queue").
/// Owns all task queues. Enforces the global CPU cap before every dispatch.
pub fn run(
    gen_rx:         Receiver<Task>,
    comp_rx:        Receiver<CompletionReport>,
    worker_txs:     Vec<Sender<Option<Task>>>,
    metrics:        Arc<Mutex<Metrics>>,
    global_cpu:     Arc<AtomicU32>,   // shared with monitor (read-only there)
    active_workers: Arc<AtomicUsize>, // shared with monitor (read-only there)
    policy:         Policy,
) {
    let num_workers = worker_txs.len();

    // FIFO uses a single arrival-order queue.
    // Optimized uses separate typed queues so it can pick across types.
    let mut fifo_queue: VecDeque<Task> = VecDeque::new();
    let mut cpu_queue:  VecDeque<Task> = VecDeque::new();
    let mut io_queue:   VecDeque<Task> = VecDeque::new();

    let mut idle: VecDeque<usize> = (0..num_workers).collect();

    // Per-worker CPU cost tracking: how much CPU each busy worker is using.
    let mut worker_cpu: Vec<u32> = vec![0; num_workers];

    let mut current_cpu: u32 = 0;
    let mut gen_done = false;

    loop {
        // --- Phase 1: ingest new tasks from the generator ---
        loop {
            match gen_rx.try_recv() {
                Ok(task) => match policy {
                    Policy::Fifo      => fifo_queue.push_back(task),
                    Policy::Optimized => match task.kind {
                        TaskKind::Cpu => cpu_queue.push_back(task),
                        TaskKind::Io  => io_queue.push_back(task),
                    },
                },
                Err(TryRecvError::Empty)        => break,
                Err(TryRecvError::Disconnected) => { gen_done = true; break; }
            }
        }

        // --- Phase 2: collect completions, free CPU and workers ---
        loop {
            match comp_rx.try_recv() {
                Ok(report) => {
                    let wid = report.worker_id;
                    current_cpu = current_cpu.saturating_sub(worker_cpu[wid]);
                    worker_cpu[wid] = 0;
                    global_cpu.store(current_cpu, Ordering::Relaxed);
                    active_workers.fetch_sub(1, Ordering::Relaxed);
                    metrics.lock().unwrap().record(report);
                    idle.push_back(wid);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        // --- Phase 3: dispatch ---
        match policy {
            Policy::Fifo => {
                // Try to dispatch the front of the queue.
                // If it doesn't fit (CPU cap), stop — this is the FIFO blocking point.
                while !idle.is_empty() {
                    match fifo_queue.front() {
                        Some(t) if current_cpu + t.cpu_percent <= 100 => {
                            let task = fifo_queue.pop_front().unwrap();
                            let wid  = idle.pop_front().unwrap();
                            worker_cpu[wid]  = task.cpu_percent;
                            current_cpu     += task.cpu_percent;
                            global_cpu.store(current_cpu, Ordering::Relaxed);
                            active_workers.fetch_add(1, Ordering::Relaxed);
                            worker_txs[wid].send(Some(task)).ok();
                        }
                        Some(_) => break, // head task won't fit; block
                        None    => break,
                    }
                }
            }
            Policy::Optimized => {
                // Batch-aware dispatch based on the prof's LP formulation:
                //
                //   Option 1: 2 CPU + 3 IO = 100% CPU, 5 workers
                //   Option 2: 1 CPU + 6 IO =  95% CPU, 7 workers  ← best throughput
                //   Option 3: 0 CPU + 8 IO =  80% CPU, 8 workers
                //
                // Choose the option each cycle based on the CPU-task ratio in the
                // queues: when CPU tasks are relatively plentiful, use Option 1 to
                // drain them; when few CPU tasks remain, switch to Option 2/3 to
                // maximise IO throughput.
                let total_pending = cpu_queue.len() + io_queue.len();
                let cpu_ratio = if total_pending > 0 {
                    cpu_queue.len() as f64 / total_pending as f64
                } else { 0.0 };

                // How many CPU tasks to target dispatching this cycle.
                let target_cpu = if cpu_ratio >= 0.30 { 2 }
                                 else if cpu_ratio >= 0.12 { 1 }
                                 else { 0 };

                let idle_snapshot: Vec<usize> = idle.drain(..).collect();
                let mut cpu_dispatched = 0usize;

                for wid in idle_snapshot {
                    let task =
                        if cpu_dispatched < target_cpu
                            && !cpu_queue.is_empty()
                            && current_cpu + 35 <= 100
                        {
                            cpu_dispatched += 1;
                            Some(cpu_queue.pop_front().unwrap())
                        } else if !io_queue.is_empty() && current_cpu + 10 <= 100 {
                            Some(io_queue.pop_front().unwrap())
                        } else if !cpu_queue.is_empty() && current_cpu + 35 <= 100 {
                            // Fallback: take a CPU task if IO queue is empty.
                            Some(cpu_queue.pop_front().unwrap())
                        } else {
                            None
                        };

                    match task {
                        Some(t) => {
                            worker_cpu[wid]  = t.cpu_percent;
                            current_cpu     += t.cpu_percent;
                            global_cpu.store(current_cpu, Ordering::Relaxed);
                            active_workers.fetch_add(1, Ordering::Relaxed);
                            worker_txs[wid].send(Some(t)).ok();
                        }
                        None => idle.push_back(wid),
                    }
                }
            }
        }

        // --- Termination ---
        let queues_empty = match policy {
            Policy::Fifo      => fifo_queue.is_empty(),
            Policy::Optimized => cpu_queue.is_empty() && io_queue.is_empty(),
        };
        if gen_done && queues_empty && idle.len() == num_workers {
            break;
        }

        thread::sleep(Duration::from_millis(1));
    }

    for tx in &worker_txs {
        tx.send(None).ok();
    }
    metrics.lock().unwrap().finalize();
}

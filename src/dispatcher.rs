use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use crate::metrics::{CompletionReport, Metrics};
use crate::task::{Task, TaskKind};

pub struct DispatchConfig {
    /// Workers 0..reserved_cpu_workers only receive CPU tasks.
    /// Set to 0 for pure priority-with-aging across both queues.
    pub reserved_cpu_workers: usize,
}

/// Runs in its own thread. Exclusively owns both queues; no lock needed on them.
pub fn run(
    gen_rx: Receiver<Task>,
    comp_rx: Receiver<CompletionReport>,
    worker_txs: Vec<Sender<Option<Task>>>,
    metrics: Arc<Mutex<Metrics>>,
    config: DispatchConfig,
) {
    let num_workers = worker_txs.len();
    let mut cpu_queue: Vec<Task> = Vec::new();
    let mut io_queue: Vec<Task> = Vec::new();
    let mut idle: VecDeque<usize> = (0..num_workers).collect();
    let mut gen_done = false;

    loop {
        // --- Phase 1: ingest new tasks from the generator ---
        loop {
            match gen_rx.try_recv() {
                Ok(task) => match task.kind {
                    TaskKind::Cpu => cpu_queue.push(task),
                    TaskKind::Io  => io_queue.push(task),
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => { gen_done = true; break; }
            }
        }

        // --- Phase 2: collect completion reports from workers ---
        loop {
            match comp_rx.try_recv() {
                Ok(report) => {
                    let wid = report.worker_id;
                    metrics.lock().unwrap().record(report);
                    idle.push_back(wid);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        // --- Phase 3: dispatch tasks to idle workers ---
        //
        // We drain the idle list each iteration, try to give every idle worker
        // a task, and put back any that had no suitable task available.
        // This lets reserved workers stay idle when their queue is empty
        // instead of incorrectly blocking all dispatch.
        let idle_snapshot: Vec<usize> = idle.drain(..).collect();
        for wid in idle_snapshot {
            let task = if wid < config.reserved_cpu_workers {
                // This worker is reserved for CPU tasks only.
                pick_from_cpu(&mut cpu_queue)
            } else {
                // General worker: pick the highest-priority task from either queue.
                pick_next(&mut cpu_queue, &mut io_queue)
            };
            match task {
                Some(t) => { worker_txs[wid].send(Some(t)).ok(); }
                None    => idle.push_back(wid), // no suitable task yet; stay idle
            }
        }

        // --- Termination: generator done, queues drained, all workers idle ---
        if gen_done
            && cpu_queue.is_empty()
            && io_queue.is_empty()
            && idle.len() == num_workers
        {
            break;
        }

        thread::sleep(Duration::from_millis(1));
    }

    // Send the shutdown sentinel so every worker thread exits its recv loop.
    for tx in &worker_txs {
        tx.send(None).ok();
    }

    metrics.lock().unwrap().finalize();
}

/// Scheduling policy: priority-based with aging across both queues.
///
/// Compares the best candidate from each queue by effective priority
/// (base priority + 1 pt per 50 ms waited) and returns the winner.
/// Aging prevents starvation: a low-priority task that waits long enough
/// will eventually outrank any later arrival.
fn pick_next(cpu_queue: &mut Vec<Task>, io_queue: &mut Vec<Task>) -> Option<Task> {
    let best_cpu = best_index(cpu_queue);
    let best_io  = best_index(io_queue);

    match (best_cpu, best_io) {
        (Some((ci, cp)), Some((ii, ip))) => {
            if cp >= ip { Some(cpu_queue.swap_remove(ci)) }
            else        { Some(io_queue.swap_remove(ii)) }
        }
        (Some((ci, _)), None) => Some(cpu_queue.swap_remove(ci)),
        (None, Some((ii, _))) => Some(io_queue.swap_remove(ii)),
        (None, None)          => None,
    }
}

/// Pick the highest-priority task from the CPU queue only (for reserved workers).
fn pick_from_cpu(cpu_queue: &mut Vec<Task>) -> Option<Task> {
    best_index(cpu_queue).map(|(i, _)| cpu_queue.swap_remove(i))
}

/// Returns (index, effective_priority) of the highest-priority task in a queue.
fn best_index(queue: &[Task]) -> Option<(usize, i32)> {
    queue
        .iter()
        .enumerate()
        .max_by_key(|(_, t)| t.effective_priority())
        .map(|(i, t)| (i, t.effective_priority()))
}

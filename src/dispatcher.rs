use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use crate::metrics::{CompletionReport, Metrics};
use crate::task::{Task, TaskKind};

/// Runs in its own thread. Owns both queues and the idle-worker list.
/// No other thread touches the queues, so no lock is needed on them.
pub fn run(
    gen_rx: Receiver<Task>,
    comp_rx: Receiver<CompletionReport>,
    worker_txs: Vec<Sender<Option<Task>>>,
    metrics: Arc<Mutex<Metrics>>,
) {
    let num_workers = worker_txs.len();
    let mut cpu_queue: Vec<Task> = Vec::new();
    let mut io_queue: Vec<Task> = Vec::new();

    // Workers start idle; the dispatcher hands out work and marks them busy.
    let mut idle: VecDeque<usize> = (0..num_workers).collect();
    let mut gen_done = false;

    loop {
        // --- Phase 1: pull new tasks from the generator channel ---
        loop {
            match gen_rx.try_recv() {
                Ok(task) => match task.kind {
                    TaskKind::Cpu => cpu_queue.push(task),
                    TaskKind::Io => io_queue.push(task),
                },
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    gen_done = true;
                    break;
                }
            }
        }

        // --- Phase 2: collect completion reports from workers ---
        loop {
            match comp_rx.try_recv() {
                Ok(report) => {
                    let wid = report.worker_id;
                    metrics.lock().unwrap().record(report);
                    idle.push_back(wid); // worker is free again
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        // --- Phase 3: assign tasks to idle workers ---
        while !idle.is_empty() {
            match pick_next(&mut cpu_queue, &mut io_queue) {
                Some(task) => {
                    let wid = idle.pop_front().unwrap();
                    worker_txs[wid].send(Some(task)).ok();
                }
                None => break, // queues are empty right now
            }
        }

        // --- Termination: generator done, queues empty, all workers idle ---
        if gen_done && cpu_queue.is_empty() && io_queue.is_empty() && idle.len() == num_workers {
            break;
        }

        // Yield briefly instead of busy-spinning the whole loop.
        thread::sleep(Duration::from_millis(1));
    }

    // Send the shutdown sentinel to every worker so their threads exit cleanly.
    for tx in &worker_txs {
        tx.send(None).ok();
    }

    metrics.lock().unwrap().finalize();
}

/// Scheduling policy: priority-based with aging.
///
/// Compares the highest-effective-priority task in each queue and returns
/// whichever scores higher. `effective_priority` adds 1 point per 50 ms
/// a task has waited, so long-waiting tasks rise above newer arrivals over
/// time — this prevents starvation without giving any task class a fixed
/// advantage.
fn pick_next(cpu_queue: &mut Vec<Task>, io_queue: &mut Vec<Task>) -> Option<Task> {
    let best_cpu = cpu_queue
        .iter()
        .enumerate()
        .max_by_key(|(_, t)| t.effective_priority())
        .map(|(i, t)| (i, t.effective_priority()));

    let best_io = io_queue
        .iter()
        .enumerate()
        .max_by_key(|(_, t)| t.effective_priority())
        .map(|(i, t)| (i, t.effective_priority()));

    match (best_cpu, best_io) {
        (Some((ci, cp)), Some((ii, ip))) => {
            if cp >= ip {
                Some(cpu_queue.swap_remove(ci))
            } else {
                Some(io_queue.swap_remove(ii))
            }
        }
        (Some((ci, _)), None) => Some(cpu_queue.swap_remove(ci)),
        (None, Some((ii, _))) => Some(io_queue.swap_remove(ii)),
        (None, None) => None,
    }
}

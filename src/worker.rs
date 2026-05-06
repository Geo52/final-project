use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crate::metrics::CompletionReport;
use crate::task::{Task, TaskKind};

/// Runs in its own thread. Blocks on its channel waiting for tasks.
/// Receives `None` as the shutdown signal and then exits cleanly.
pub fn run(id: usize, rx: Receiver<Option<Task>>, comp_tx: Sender<CompletionReport>) {
    loop {
        match rx.recv() {
            Ok(Some(task)) => {
                let start_time = Instant::now();

                match task.kind {
                    TaskKind::Cpu => {
                        // Simulate CPU-bound work: spin so we actually consume CPU time.
                        let deadline = start_time + Duration::from_millis(task.duration_ms);
                        let mut x = 0u64;
                        while Instant::now() < deadline {
                            x = x.wrapping_add(1);
                        }
                        let _ = x;
                    }
                    TaskKind::Io => {
                        // Simulate IO-bound work: block the thread as real IO would.
                        thread::sleep(Duration::from_millis(task.duration_ms));
                    }
                }

                let end_time = Instant::now();
                let report = CompletionReport {
                    task_id: task.id,
                    worker_id: id,
                    kind: task.kind,
                    arrival_time: task.arrival_time,
                    start_time,
                    end_time,
                };
                if comp_tx.send(report).is_err() {
                    break;
                }
            }
            Ok(None) | Err(_) => break, // None = shutdown signal; Err = dispatcher gone
        }
    }
}

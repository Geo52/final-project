use std::sync::mpsc::{Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crate::metrics::CompletionReport;
use crate::task::{Task, TASK_DURATION_MS};

/// Blocks waiting for tasks. Both CPU and IO tasks sleep TASK_DURATION_MS to
/// simulate execution time; the distinction is the cpu_percent they carry,
/// which the manager uses to enforce the global CPU cap.
pub fn run(id: usize, rx: Receiver<Option<Task>>, comp_tx: Sender<CompletionReport>) {
    loop {
        match rx.recv() {
            Ok(Some(task)) => {
                let start_time = Instant::now();
                thread::sleep(Duration::from_millis(TASK_DURATION_MS));
                let end_time = Instant::now();

                let report = CompletionReport {
                    task_id:      task.id,
                    worker_id:    id,
                    kind:         task.kind,
                    arrival_time: task.arrival_time,
                    start_time,
                    end_time,
                };
                if comp_tx.send(report).is_err() {
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
}

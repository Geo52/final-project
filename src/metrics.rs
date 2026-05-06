use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::task::TaskKind;

/// Sent from a worker to the dispatcher when a task finishes.
pub struct CompletionReport {
    pub task_id: u64,
    pub worker_id: usize,
    pub kind: TaskKind,
    pub arrival_time: Instant,
    pub start_time: Instant,
    pub end_time: Instant,
}

/// Accumulates per-task completion data.
/// Written exclusively by the dispatcher thread; read by main after all threads join.
pub struct Metrics {
    reports: Vec<CompletionReport>,
    worker_busy_ms: Vec<u64>,
    run_start: Instant,
    run_end: Option<Instant>,
    /// Shared with the monitor thread so it can print live progress without locking.
    pub counter: Arc<AtomicUsize>,
}

impl Metrics {
    pub fn new(num_workers: usize, counter: Arc<AtomicUsize>) -> Self {
        Self {
            reports: Vec::new(),
            worker_busy_ms: vec![0; num_workers],
            run_start: Instant::now(),
            run_end: None,
            counter,
        }
    }

    pub fn record(&mut self, report: CompletionReport) {
        let busy = report.end_time.duration_since(report.start_time).as_millis() as u64;
        self.worker_busy_ms[report.worker_id] += busy;
        self.counter.fetch_add(1, Ordering::Relaxed);
        self.reports.push(report);
    }

    pub fn finalize(&mut self) {
        self.run_end = Some(Instant::now());
    }

    pub fn print_summary(&self) {
        let total = self.reports.len();
        if total == 0 {
            println!("No tasks completed.");
            return;
        }

        let makespan_ms = self
            .run_end
            .unwrap_or_else(Instant::now)
            .duration_since(self.run_start)
            .as_millis() as u64;

        let mut total_wait: u64 = 0;
        let mut total_turnaround: u64 = 0;
        let mut max_wait: u64 = 0;
        let mut max_wait_id: u64 = 0;
        let mut cpu_wait_sum: u64 = 0;
        let mut io_wait_sum: u64 = 0;
        let mut cpu_count: usize = 0;
        let mut io_count: usize = 0;

        for r in &self.reports {
            let wait = r.start_time.duration_since(r.arrival_time).as_millis() as u64;
            let turnaround = r.end_time.duration_since(r.arrival_time).as_millis() as u64;
            total_wait += wait;
            total_turnaround += turnaround;
            if wait > max_wait {
                max_wait = wait;
                max_wait_id = r.task_id;
            }
            match r.kind {
                TaskKind::Cpu => { cpu_wait_sum += wait; cpu_count += 1; }
                TaskKind::Io  => { io_wait_sum  += wait; io_count  += 1; }
            }
        }

        let avg_wait       = total_wait      / total as u64;
        let avg_turnaround = total_turnaround / total as u64;
        let cpu_avg_wait   = if cpu_count > 0 { cpu_wait_sum / cpu_count as u64 } else { 0 };
        let io_avg_wait    = if io_count  > 0 { io_wait_sum  / io_count  as u64 } else { 0 };

        let num_workers = self.worker_busy_ms.len() as u64;
        let capacity_ms = makespan_ms.saturating_mul(num_workers);
        let total_busy_ms: u64 = self.worker_busy_ms.iter().sum();
        let utilization_pct = if capacity_ms > 0 { 100 * total_busy_ms / capacity_ms } else { 0 };

        let fairness_gap = (cpu_avg_wait as i64 - io_avg_wait as i64).unsigned_abs();

        println!("=== Summary Statistics ===");
        println!("Total tasks completed  : {total}");
        println!("  CPU tasks            : {cpu_count}");
        println!("  IO  tasks            : {io_count}");
        println!("Makespan               : {makespan_ms} ms");
        println!("Avg wait time          : {avg_wait} ms");
        println!("Avg turnaround time    : {avg_turnaround} ms");
        println!("Max wait time          : {max_wait} ms  (task {max_wait_id})");
        println!("Worker utilization     : {utilization_pct}%");
        println!(
            "Fairness gap           : {fairness_gap} ms  \
             (CPU avg {cpu_avg_wait} ms  vs  IO avg {io_avg_wait} ms)"
        );
    }
}

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

/// Accumulates completion data. Written by the dispatcher, read by main after all threads join.
pub struct Metrics {
    pub reports: Vec<CompletionReport>,
    pub worker_busy_ms: Vec<u64>,
    pub run_start: Instant,
    pub run_end: Option<Instant>,
}

impl Metrics {
    pub fn new(num_workers: usize) -> Self {
        Self {
            reports: Vec::new(),
            worker_busy_ms: vec![0; num_workers],
            run_start: Instant::now(),
            run_end: None,
        }
    }

    pub fn record(&mut self, report: CompletionReport) {
        let busy = report.end_time.duration_since(report.start_time).as_millis() as u64;
        self.worker_busy_ms[report.worker_id] += busy;
        self.reports.push(report);
    }

    pub fn finalize(&mut self) {
        self.run_end = Some(Instant::now());
    }

    pub fn print_summary(&self) {
        let total = self.reports.len();
        println!("Tasks completed : {total}");

        let cpu = self.reports.iter().filter(|r| r.kind == TaskKind::Cpu).count();
        let io = self.reports.iter().filter(|r| r.kind == TaskKind::Io).count();
        println!("  CPU           : {cpu}");
        println!("  IO            : {io}");

        let makespan_ms = self
            .run_end
            .unwrap_or_else(Instant::now)
            .duration_since(self.run_start)
            .as_millis();
        println!("Makespan        : {makespan_ms} ms");
    }
}

use std::time::Instant;

use crate::task::TaskKind;

pub struct CompletionReport {
    #[allow(dead_code)]
    pub task_id:      u64,
    pub worker_id:    usize,
    pub kind:         TaskKind,
    pub arrival_time: Instant,
    pub start_time:   Instant,
    pub end_time:     Instant,
}

/// Snapshot recorded by the monitor thread every 10 ms.
pub struct MonitorSample {
    pub cpu_percent:     u32,
    pub active_workers:  usize,
}

pub struct Metrics {
    pub reports:     Vec<CompletionReport>,
    pub samples:     Vec<MonitorSample>,  // from monitor thread
    run_start:       Instant,
    run_end:         Option<Instant>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            reports:   Vec::new(),
            samples:   Vec::new(),
            run_start: Instant::now(),
            run_end:   None,
        }
    }

    pub fn record(&mut self, report: CompletionReport) {
        self.reports.push(report);
    }

    pub fn add_sample(&mut self, sample: MonitorSample) {
        self.samples.push(sample);
    }

    pub fn finalize(&mut self) {
        self.run_end = Some(Instant::now());
    }

    pub fn print_summary(&self, label: &str) {
        let total = self.reports.len();
        let makespan_ms = self
            .run_end
            .unwrap_or_else(Instant::now)
            .duration_since(self.run_start)
            .as_millis() as u64;

        let cpu_count = self.reports.iter().filter(|r| r.kind == TaskKind::Cpu).count();
        let io_count  = self.reports.iter().filter(|r| r.kind == TaskKind::Io).count();

        let avg_cpu = if self.samples.is_empty() { 0 } else {
            self.samples.iter().map(|s| s.cpu_percent as u64).sum::<u64>()
                / self.samples.len() as u64
        };
        let avg_workers = if self.samples.is_empty() { 0 } else {
            self.samples.iter().map(|s| s.active_workers as u64).sum::<u64>()
                / self.samples.len() as u64
        };
        let peak_cpu = self.samples.iter().map(|s| s.cpu_percent).max().unwrap_or(0);

        let avg_wait_ms = if total == 0 { 0 } else {
            self.reports.iter()
                .map(|r| r.start_time.duration_since(r.arrival_time).as_millis() as u64)
                .sum::<u64>() / total as u64
        };
        let avg_turnaround_ms = if total == 0 { 0 } else {
            self.reports.iter()
                .map(|r| r.end_time.duration_since(r.arrival_time).as_millis() as u64)
                .sum::<u64>() / total as u64
        };

        println!("--- {label} ---");
        println!("Total tasks completed : {total}  (CPU {cpu_count} / IO {io_count})");
        println!("Total runtime         : {makespan_ms} ms");
        println!("Avg wait time         : {avg_wait_ms} ms");
        println!("Avg turnaround time   : {avg_turnaround_ms} ms");
        println!("Avg CPU usage         : {avg_cpu}%  (peak {peak_cpu}%)");
        println!("Avg active workers    : {avg_workers} / 8");
    }

    pub fn total_runtime_ms(&self) -> u64 {
        self.run_end
            .unwrap_or_else(Instant::now)
            .duration_since(self.run_start)
            .as_millis() as u64
    }

    pub fn avg_cpu(&self) -> u64 {
        if self.samples.is_empty() { return 0; }
        self.samples.iter().map(|s| s.cpu_percent as u64).sum::<u64>()
            / self.samples.len() as u64
    }
}

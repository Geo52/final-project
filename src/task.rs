use std::time::Instant;

pub const CPU_PERCENT: u32 = 35;
pub const IO_PERCENT: u32 = 10;
pub const TASK_DURATION_MS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Cpu,
    Io,
}

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Cpu => write!(f, "CPU"),
            TaskKind::Io  => write!(f, "IO "),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id:           u64,
    pub arrival_time: Instant,
    pub kind:         TaskKind,
    /// CPU load this task places on the system while executing.
    pub cpu_percent:  u32,
}

impl Task {
    pub fn new_cpu(id: u64) -> Self {
        Self { id, arrival_time: Instant::now(), kind: TaskKind::Cpu, cpu_percent: CPU_PERCENT }
    }

    pub fn new_io(id: u64) -> Self {
        Self { id, arrival_time: Instant::now(), kind: TaskKind::Io, cpu_percent: IO_PERCENT }
    }
}

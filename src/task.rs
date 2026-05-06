use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskKind {
    Cpu,
    Io,
}

impl std::fmt::Display for TaskKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskKind::Cpu => write!(f, "CPU"),
            TaskKind::Io => write!(f, "IO "),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub arrival_time: Instant,
    pub kind: TaskKind,
    pub duration_ms: u64,
    pub priority: i32,
}

impl Task {
    /// Effective priority grows with wait time to prevent starvation.
    /// One extra priority point is added per 50 ms spent waiting in the queue.
    pub fn effective_priority(&self) -> i32 {
        let waited_ms = self.arrival_time.elapsed().as_millis() as i32;
        self.priority + waited_ms / 50
    }
}

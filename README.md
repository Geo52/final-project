# Concurrent Task Dispatcher

A concurrent task dispatcher written in Rust that simulates a stream of incoming CPU-bound and IO-bound work, routes tasks through typed queues, and dispatches them to a bounded worker pool using a priority-based scheduling policy with aging.

---

## How to build

```
cargo build --release
```

Requires Rust 1.65 or later. No external tools needed.

---

## How to run

```
cargo run -- [workload] [policy]
```

| Argument   | Values                  | Default    |
|------------|-------------------------|------------|
| `workload` | `balanced`, `stressed`  | `balanced` |
| `policy`   | `priority`, `reserved`  | `priority` |

---

## Command examples

```bash
# Experiment A: balanced workload, pure priority + aging policy
cargo run -- balanced priority

# Experiment A: balanced workload, shorthand (both args optional)
cargo run

# Experiment B: stressed workload, pure priority + aging
cargo run -- stressed

# Experiment B: stressed workload, 2 workers reserved for CPU tasks
cargo run -- stressed reserved

# Show usage
cargo run -- --help
```

Release build runs faster and gives more realistic CPU-task timings:

```bash
cargo run --release -- stressed reserved
```

---

## Summary of design

The system has five kinds of concurrent actors:

- **Generator thread** — produces 600 tasks in arrival-time order using a fixed seed, then drops its sender to signal completion.
- **Dispatcher thread** — owns a CPU queue and an IO queue (no lock needed; it is the only thread that touches them). Each iteration it ingests new tasks, collects worker completions, dispatches tasks to idle workers, and checks the termination condition.
- **Worker threads (×8)** — each blocks on its own channel waiting for a task. CPU tasks are simulated with a busy-wait loop; IO tasks use `thread::sleep`. Workers send a `CompletionReport` back to the dispatcher on completion and exit cleanly when they receive `None`.
- **Monitor thread** — reads a lock-free `AtomicUsize` counter every 500 ms and prints live progress without interfering with the dispatch loop.
- **Main thread** — wires everything together, joins threads in natural order, and prints the final summary.

Channels carry tasks from generator → dispatcher and completion reports from workers → dispatcher. The only `Arc<Mutex<>>` is around `Metrics`, which the dispatcher writes and main reads after all threads have joined.

### Scheduling policies

**`priority`** — picks the task with the highest *effective priority* from either queue, where:

```
effective_priority = base_priority + (ms_waited / 50)
```

Aging adds one priority point per 50 ms in the queue, preventing starvation.

**`reserved`** — same formula, but workers 0–1 only pull from the CPU queue. Workers 2–7 pull from either queue. This guarantees IO tasks always have access to at least 6 workers even under a CPU-heavy burst.

---

## Summary of experiments

| Metric               | Balanced (priority) | Stressed (priority) | Stressed (reserved) |
|----------------------|--------------------:|--------------------:|--------------------:|
| Makespan             | 2001 ms             | 2733 ms             | 2737 ms             |
| Avg wait time        | 0 ms                | 1248 ms             | 1249 ms             |
| Avg turnaround       | 12 ms               | 1283 ms             | 1284 ms             |
| Max wait time        | 15 ms               | 2433 ms             | 2434 ms             |
| Worker utilization   | 46%                 | 97%                 | 97%                 |
| Fairness gap         | 0 ms                | 30 ms               | **8 ms**            |

The balanced workload is easy for 8 workers: tasks arrive spread over 2 s, workers are never all busy at once (46% utilization), and wait times are negligible. The stressed workload floods the system with 502 CPU tasks in a 400 ms burst, driving utilization to 97% and average wait above 1.2 s. Switching to the reserved policy cuts the CPU-vs-IO fairness gap from 30 ms to 8 ms with no meaningful cost to makespan.

---

## Tool Use Disclosure

**Tools used:** Claude Code (Anthropic AI assistant).

**Kind of help:** The assistant helped write and structure Rust code for the dispatcher, worker, generator, and metrics modules across a series of incremental steps following the layered build order suggested in the project handout.

**Example of advice accepted:** The suggestion to drain-and-requeue the idle-worker list each dispatch iteration (rather than popping one worker at a time) was accepted. This makes the reserved-worker policy correct: each worker's identity is known before a task is selected for it, so CPU-only workers are never sent IO tasks.

**Example of advice rejected or fixed:** The assistant initially proposed using `idle.is_empty()` as part of the termination condition. This is wrong: `idle.is_empty()` is true whenever all workers are busy, which includes the case where workers still have tasks in flight. The correct condition is `idle.len() == num_workers` (all workers have returned to the idle pool), which is what the final code uses.

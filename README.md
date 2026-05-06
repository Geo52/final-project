# Concurrent Task Dispatcher

A concurrent task dispatcher in Rust that simulates an OS-style scheduler.
A generator thread produces 1000 tasks (70% IO-bound, 30% CPU-bound) at fixed
20 ms intervals. A manager-queue thread routes tasks through typed queues, enforces
a global 100% CPU cap, and dispatches work to a pool of 8 worker threads.
Two scheduling policies are compared: FIFO and an Optimized batch-aware policy.

---

## How to build

```
cargo build --release
```

Requires Rust 1.65 or later.

---

## How to run

```
cargo run -- [workload]
```

| Argument   | Values           | Default  | Description               |
|------------|------------------|----------|---------------------------|
| `workload` | `70-30`, `80-20` | `70-30`  | IO/CPU task ratio         |

Running with no arguments executes **both simulations** (FIFO then Optimized)
on the same workload and prints a comparison.

---

## Command examples

```bash
# Both simulations, 70% IO / 30% CPU workload (default)
cargo run

# Both simulations, 80% IO / 20% CPU workload
cargo run -- 80-20

# Release build (faster CPU simulation)
cargo run --release
```

---

## Thread structure (11 threads total)

| Thread          | Count | Role                                                         |
|-----------------|-------|--------------------------------------------------------------|
| Main            | 1     | Wires everything, joins threads, prints summary              |
| Generator       | 1     | Sends 1000 tasks at 20 ms intervals, then drops sender       |
| Manager queue   | 1     | Owns task queues, enforces CPU cap, dispatches to workers    |
| Workers         | 8     | Execute tasks (both types sleep 200 ms), report completion   |
| Monitor         | 1     | Samples CPU% and active-worker count every 10 ms             |

---

## Task model

| Field        | CPU task | IO task |
|--------------|----------|---------|
| Duration     | 200 ms   | 200 ms  |
| CPU load     | 35%      | 10%     |

The global CPU usage is the sum of all currently running tasks' loads.
The manager never dispatches a task that would push the total above 100%.

---

## Scheduling policies

**FIFO** — Tasks are dispatched in strict arrival order. If the head task would
exceed the CPU cap, the manager waits (head-of-line blocking). This can cause IO
tasks to stall behind CPU tasks even when CPU headroom exists.

**Optimized** — Separate CPU and IO queues. Each dispatch cycle, the manager
selects a batch configuration based on the current CPU/IO ratio in the queues,
targeting the LP-optimal mix of options:

- Option 1: 2 CPU + 3 IO = 100% CPU, 5 workers
- Option 2: 1 CPU + 6 IO = 95%  CPU, 7 workers  ← best throughput
- Option 3: 0 CPU + 8 IO = 80%  CPU, 8 workers

When the CPU-task ratio ≥ 30%, it targets 2 CPU tasks per batch (Option 1).
As the CPU queue drains (ratio 12–30%), it shifts to 1 CPU + fill IO (Option 2).
When the CPU queue empties, it runs pure IO (Option 3).
No head-of-line blocking — IO tasks can always dispatch if CPU headroom allows.

---

## Experiment results (70/30 workload)

| Metric              | FIFO    | Optimized |
|---------------------|--------:|----------:|
| Total runtime       | 39210 ms| 41910 ms  |
| Avg wait time       | 9206 ms | **5452 ms** |
| Avg turnaround      | 9406 ms | **5652 ms** |
| Avg CPU usage       | 89%     | 83%       |
| Avg active workers  | 5 / 8   | 4 / 8     |

**Key finding:** FIFO achieves slightly better throughput (39.2 s vs 41.9 s) because
it naturally runs IO-heavy batches that keep CPU close to 89%. Optimized trades a
small throughput cost for a 41% reduction in average wait time (9.2 s → 5.5 s) by
eliminating head-of-line blocking. This is the classic throughput-vs-latency
trade-off: FIFO is better if you care about finishing all tasks fastest; Optimized
is better if individual task responsiveness matters.

Full output: see `experiment_output.txt`.

---

## Summary of design

- **Channels** carry all task data (generator → manager, manager → workers, workers → manager)
- **Arc<Mutex<Metrics>>** is the only shared mutable state; the manager writes, main reads after join
- **Arc<AtomicU32>** tracks live CPU% for the monitor (no lock needed — manager is the sole writer)
- **Arc<AtomicUsize>** tracks live active-worker count for the monitor
- Task queues are plain local data inside the manager thread — no lock needed

---

## Tool Use Disclosure

**Tools used:** Claude Code (Anthropic AI assistant).

**Kind of help:** Helped implement and iteratively refine all Rust source files
(generator, manager queue, workers, metrics, monitor) following the project spec.

**Example of advice accepted:** Implementing the `idle.len() == num_workers` termination
condition. The assistant explained that `idle.is_empty()` (the naive check) fires as
soon as all tasks are dispatched, while workers are still executing — causing the
manager to shut down before collecting their final completion reports.

**Example of advice rejected or fixed:** The assistant initially proposed a
"prefer CPU tasks first" Optimized policy. This turned out to be worse than FIFO
because it limited worker parallelism for the IO-heavy workload. The policy was
reworked to an LP-based batch-selection approach that adapts to the current
CPU/IO queue ratio, which is the approach described in the report.

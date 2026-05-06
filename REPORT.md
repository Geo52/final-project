# Design Report — Concurrent Task Dispatcher

## 1. Threads and major components

The system runs six kinds of threads concurrently.

**Generator** (`src/generator.rs`) Pre-computes 600 arrival offsets with a fixed seed, sorts them, and sleeps until each task's arrival time before sending it down a channel. Dropping the sender at the end is the shutdown signal to the dispatcher.

**Dispatcher** (`src/dispatcher.rs`) The scheduling brain. It owns both queues directly — no lock is needed because no other thread touches them. Each iteration runs three phases in order: ingest new tasks from the generator channel, collect completion reports from workers, then dispatch tasks to idle workers. A 1 ms yield between iterations prevents pure busy-spinning without adding meaningful latency.

**Workers ×8** (`src/worker.rs`) Each worker blocks on `rx.recv()` waiting for its next task. CPU tasks run a busy-wait spin to actually consume CPU time; IO tasks call `thread::sleep` to simulate blocking IO. After execution the worker sends a `CompletionReport` back and immediately waits again. Workers exit cleanly when they receive `None`.

**Monitor** (`src/main.rs`) A lightweight thread that reads an `AtomicUsize` completion counter every 500 ms and prints progress. It holds no locks and writes no shared state.

**Main** (`src/main.rs`) Constructs all channels, spawns all threads, joins them in natural completion order (generator → dispatcher → workers → monitor), then prints the final summary.

```
Generator ──(Task channel)──► Dispatcher ──(per-worker channels)──► Workers ×8
                                   ▲                                     │
                                   └────────(CompletionReport chan)───────┘
                                   │
                              Arc<Mutex<Metrics>>
                                   │
                                 Main (reads after join)

Monitor ──reads──► AtomicUsize counter (written by Dispatcher.record())
```

---

## 2. What data is shared and how it is protected

| Shared data          | Protection              | Reason                                                    |
|----------------------|-------------------------|-----------------------------------------------------------|
| `Metrics` struct     | `Arc<Mutex<Metrics>>`   | Dispatcher writes on every completion; main reads at end  |
| Completion counter   | `Arc<AtomicUsize>`      | Single increment per completion; monitor only reads it    |
| Monitor stop flag    | `Arc<AtomicBool>`       | Main sets it once after joining all other threads         |

The two queues (`cpu_queue`, `io_queue`) are plain `Vec<Task>` values owned by the dispatcher thread. Because only one thread ever touches them, no synchronization is needed.

---

## 3. Where channels are used and why

| Channel                        | Type                     | Why a channel                                              |
|--------------------------------|--------------------------|------------------------------------------------------------|
| Generator → Dispatcher         | `mpsc::channel<Task>`    | Generator and dispatcher run at different rates; the channel decouples them and its close signal (sender drop) is the natural shutdown notification |
| Dispatcher → Worker *i*        | `mpsc::channel<Option<Task>>` | Each worker needs its own queue so the dispatcher can target a specific idle worker; `None` serves as a typed shutdown sentinel |
| Workers → Dispatcher           | `mpsc::channel<CompletionReport>` | Many workers share one receiver; `mpsc` gives this for free and avoids polling each worker individually |

Channels are preferred over shared state here because the data has a clear direction of flow (producer → consumer) and ownership transfers cleanly. The alternative — a shared `Mutex<VecDeque<Task>>` that workers pull from — would mean the dispatcher cannot control which worker gets which task, making the reserved-worker policy impossible.

---

## 4. Where shared state is used and why

`Arc<Mutex<Metrics>>` is the only shared mutable state. It is shared rather than sent through a channel because main needs to read the final metrics after the dispatcher thread has exited, not receive them through a message. The lock is only acquired on each completion event (at most 600 times total) and once at the end, so contention is negligible.

The `AtomicUsize` counter is shared state without a lock because the monitor only needs an approximate live count. Relaxed ordering is sufficient — a value that is one or two completions stale is fine for a progress display.

---

## 5. Scheduling policy

**Policy: priority-based with aging, optionally with reserved workers.**

Each task has a `priority` field (1–10, random at generation time). When the dispatcher picks the next task, it does not use base priority directly. Instead it computes an *effective priority* for every queued task:

```
effective_priority = base_priority + (milliseconds_waited / 50)
```

The dispatcher picks whichever task across both queues has the highest effective priority. Ties go to the CPU queue (the `>=` comparison).

**Why aging?** Without it, a stream of high-priority arrivals would permanently bury low-priority tasks. With aging, every task gains one priority point per 50 ms of waiting, so a low-priority task that has waited 500 ms has an effective priority 10 points higher than its base — enough to outrank even a freshly arrived priority-10 task.

**Reserved-worker extension.** Workers 0–1 only pull from the CPU queue; workers 2–7 pull from either queue using the same aging formula. The dispatcher decides which queue to pull from *after* selecting which idle worker to dispatch to, not before. This is implemented in the drain-and-requeue dispatch loop:

```rust
for wid in idle.drain(..).collect::<Vec<_>>() {
    let task = if wid < config.reserved_cpu_workers {
        pick_from_cpu(&mut cpu_queue)
    } else {
        pick_next(&mut cpu_queue, &mut io_queue)
    };
    match task {
        Some(t) => worker_txs[wid].send(Some(t)).ok(),
        None    => idle.push_back(wid),   // no suitable task; worker stays idle
    }
}
```

---

## 6. What improved because of the policy

Aging prevents starvation. In a pure FIFO system, a burst of high-priority tasks arriving after a low-priority task is queued would delay that low-priority task indefinitely. With aging, any task queued long enough will eventually win the priority comparison.

The reserved-worker extension improves fairness under skewed workloads. In the stressed experiment (85% CPU tasks), the pure priority policy produced a CPU-vs-IO fairness gap of 30 ms. With 2 workers reserved for CPU tasks, IO tasks always had access to the remaining 6 workers, cutting the fairness gap to 8 ms — a 73% reduction — with no increase in makespan.

---

## 7. What became worse or more complicated

**Reserved workers add idle time.** If the CPU queue drains faster than new CPU tasks arrive, the 2 reserved workers sit idle even though IO tasks are queued. This wastes capacity. In the balanced experiment this effect was not visible because workers were never all busy simultaneously, but in a workload where CPU tasks are rare and IO tasks dominate, reserved CPU workers would hurt throughput.

**Dispatch order now matters.** The original dispatch loop popped tasks first, then picked a worker. With reserved workers, the worker identity must be known before selecting a task. This required restructuring to the drain-and-requeue approach, which is slightly harder to reason about.

**The aging constant is a tuning knob.** Setting `AGING_DIVISOR = 50` ms was chosen by inspection. A smaller value ages tasks faster (better starvation protection, but low-priority tasks rise too quickly); a larger value ages them slower (more faithful to base priority, but starvation risk returns). The right value depends on workload characteristics that are not known at compile time.

---

## 8. Concurrency bug encountered during development

**The termination condition used `idle.is_empty()` instead of `idle.len() == num_workers`.**

The original draft checked:

```rust
if gen_done && cpu_queue.is_empty() && io_queue.is_empty() && idle.is_empty() {
    break;
}
```

`idle.is_empty()` is true whenever all workers are currently dispatched — including the case where they are still executing tasks and have not yet sent their completion reports. This caused the dispatcher to break out of its loop early, send `None` to all workers, and drop the completion-report receiver before all workers had finished. Workers that tried to send their final `CompletionReport` hit a disconnected channel, the reports were lost, and the final metrics were short.

## 9. How it was fixed

The condition was changed to:

```rust
if gen_done && cpu_queue.is_empty() && io_queue.is_empty() && idle.len() == num_workers {
    break;
}
```

`idle.len() == num_workers` is only true when every worker has sent its completion report *and* the dispatcher has processed it (moved the worker back into the idle set). This is the correct quiescence condition: the system is truly done only when all workers have returned to idle, not merely when all tasks have been dispatched.

---

## 10. Where starvation or unfairness could still happen

**Long CPU tasks blocking IO tasks in the general pool.** If the 6 general workers each pick up a 60 ms CPU task simultaneously, IO tasks with even very high effective priorities must wait at least 60 ms before a general worker frees up. During that window, the 2 reserved workers are also busy with CPU tasks. IO tasks can still face multi-hundred-millisecond waits in a sustained CPU burst.

**Aging rate vs. task duration mismatch.** If a 60 ms CPU task arrives just before a 5 ms IO task that has already waited 250 ms, the IO task has aged to `base + 5` priority points extra. A CPU task with base priority 10 still beats it by up to 5 points for the first 250 ms. A workload that continuously injects fresh high-priority CPU tasks could suppress IO tasks for as long as the burst lasts.

**Reserved workers idle during IO-heavy workloads.** If the workload flips to mostly IO, the 2 reserved workers sit idle indefinitely. They cannot be reassigned at runtime — the reservation is static. A future improvement would be *work stealing*: allow a reserved worker that has been idle for more than T ms to pull from the IO queue as a fallback.

---

## Metrics collected

| Metric              | Description                                        |
|---------------------|----------------------------------------------------|
| Total tasks         | Count of all completions (CPU + IO separately)     |
| Makespan            | Wall-clock time from first task arrival to last completion |
| Avg wait time       | Mean of `start_time − arrival_time` across all tasks |
| Avg turnaround      | Mean of `end_time − arrival_time` across all tasks |
| Max wait time       | Worst-case wait, with the task ID that experienced it |
| Worker utilization  | `total_busy_ms / (makespan × num_workers)` — fraction of worker-time spent executing |
| Fairness gap        | Difference between CPU avg wait and IO avg wait    |

---

## Experiment results

### Experiment A — Balanced workload

Configuration: 600 tasks, 50% CPU / 50% IO, durations 5–20 ms, arrivals spread uniformly over 2 s.

| Metric             | Result                |
|--------------------|-----------------------|
| Makespan           | 2001 ms               |
| Avg wait time      | 0 ms                  |
| Avg turnaround     | 12 ms                 |
| Max wait time      | 15 ms (task 324)      |
| Worker utilization | 46%                   |
| Fairness gap       | 0 ms                  |

**Interpretation.** Tasks arrive slowly enough that workers are rarely all busy. The 46% utilization confirms that workers spend more time waiting than working. Wait times are effectively zero because the dispatcher always has an idle worker ready. Fairness is perfect — both task types are served as fast as they arrive.

### Experiment B — Stressed workload, compared across policies

Configuration: 600 tasks, 85% CPU / 15% IO, durations 10–60 ms, 70% of tasks burst within the first 80 ms of a 400 ms arrival window.

| Metric             | Priority (no reservation) | Priority + Reserved workers |
|--------------------|---------------------------|-----------------------------|
| Makespan           | 2733 ms                   | 2737 ms                     |
| Avg wait time      | 1248 ms                   | 1249 ms                     |
| Avg turnaround     | 1283 ms                   | 1284 ms                     |
| Max wait time      | 2433 ms                   | 2434 ms                     |
| Worker utilization | 97%                       | 97%                         |
| Fairness gap       | **30 ms**                 | **8 ms**                    |

**Interpretation.** The burst delivers ~420 tasks within 80 ms, instantly saturating all 8 workers (97% utilization). Average wait rises to over 1.2 s because tasks spend most of their lifecycle in a queue. Makespan is essentially unchanged between policies — the system is compute-bound either way. The important difference is the fairness gap: reserving 2 workers for CPU tasks guarantees IO tasks always have 6 general workers available, reducing their relative disadvantage from 30 ms to 8 ms. The remaining 8 ms gap reflects the fact that CPU tasks, being 5× more numerous, still dominate general-worker attention through the aging-based priority system.

---

## Lessons learned

1. **Termination conditions in concurrent pipelines require careful thought.** "All queues empty" is not the same as "all work done." Work in flight — tasks executing but not yet reported — must be accounted for before declaring quiescence.

2. **Knowing the worker before selecting the task matters for typed policies.** The reserved-worker policy forced a restructuring of the dispatch loop. The key insight is that task selection must be conditioned on worker identity, not the reverse.

3. **Channels transfer ownership; shared state requires explicit coordination.** Using channels for task delivery meant no lock was needed on the queues. The only `Mutex` in the system guards the metrics, which genuinely needed to be read by a different thread after the writer was done.

4. **Aging is effective but its rate is a policy choice.** A divisor of 50 ms worked well for durations in the 5–60 ms range. For workloads with longer durations, a larger divisor (less aggressive aging) would be appropriate to preserve priority ordering without prematurely elevating low-priority tasks.

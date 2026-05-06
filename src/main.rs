mod task;
mod generator;

use std::sync::mpsc;
use std::thread;

use generator::WorkloadConfig;

const NUM_TASKS: u64 = 600;
const SEED: u64 = 42;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let experiment = args.get(1).map(|s| s.as_str()).unwrap_or("balanced");

    let config = match experiment {
        "stressed" => {
            println!("Experiment: Stressed workload (85% CPU, burst arrivals)");
            WorkloadConfig::stressed(NUM_TASKS, SEED)
        }
        _ => {
            println!("Experiment: Balanced workload (50% CPU / 50% IO, uniform arrivals)");
            WorkloadConfig::balanced(NUM_TASKS, SEED)
        }
    };

    println!("Generating {} tasks (seed={SEED})...\n", config.num_tasks);

    let (tx, rx) = mpsc::channel();

    let gen_handle = thread::spawn(move || generator::run(tx, config));

    // For now: collect and print a sample to verify generation works.
    let mut cpu_count = 0u64;
    let mut io_count = 0u64;
    let mut tasks = Vec::new();

    while let Ok(task) = rx.recv() {
        match task.kind {
            task::TaskKind::Cpu => cpu_count += 1,
            task::TaskKind::Io  => io_count  += 1,
        }
        tasks.push(task);
    }

    gen_handle.join().expect("generator panicked");

    println!("Tasks received : {}", tasks.len());
    println!("  CPU          : {cpu_count}");
    println!("  IO           : {io_count}");

    // Print the first 10 tasks as a sanity check.
    println!("\nFirst 10 tasks:");
    for t in tasks.iter().take(10) {
        println!(
            "  id={:3}  kind={}  duration={:3}ms  priority={}",
            t.id, t.kind, t.duration_ms, t.priority
        );
    }
}

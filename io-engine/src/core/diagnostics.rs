use crate::core::{MayastorCliArgs, Reactor};
use async_process::Command;
use rstack::TraceOptions;
use std::env;

/// Get command path from process CLI arguments.
fn get_io_agent_path() -> String {
    env::args().next().as_ref().map(String::from).unwrap()
}

/// Dump stack for current I/O engine instance and log it.
async fn dump_self_stack() {
    let pid = std::process::id();

    info!(pid, "Collecting stack for I/O agent process");

    let out = Command::new(get_io_agent_path())
        .arg("--diagnose-stack")
        .arg(pid.to_string())
        .output()
        .await;

    match out {
        Err(error) => {
            error!(
                %error,
                "Failed to collect process stack"
            );
        }
        Ok(output) => {
            let l = String::from_utf8(output.stdout).unwrap();
            l.split('\n').for_each(|s| {
                if !s.is_empty() {
                    if s.starts_with("thread ") {
                        info!("\n{}", s);
                    } else {
                        info!("{}", s);
                    }
                }
            });
            info!("Process stack collected successfully");
        }
    }
}

/// Dump detailed diagnostic information for the frozen reactor.
/// As of now print only basic information about the reactor,
/// in the future might print stack traces too.
pub fn diagnose_reactor(reactor: &Reactor) {
    info!(
        core=reactor.core(),
        tid=reactor.tid(),
        state=%reactor.get_state(),
        "Reactor is frozen"
    );

    // Spawn a task to perform stack collection.
    tokio::spawn(async move {
        dump_self_stack().await;
    });
}

/// Collect sracktraces for all stack frames for all threads in target process
/// and dump it to stdout.
fn collect_process_stack(pid: u32) -> Result<(), Box<dyn std::error::Error>> {
    let process = TraceOptions::new()
        .thread_names(true)
        .symbols(true)
        .trace(pid)?;

    for thread in process.threads() {
        println!(
            "thread {} - {}",
            thread.id(),
            thread.name().unwrap_or("<unknown>")
        );

        for frame in thread.frames() {
            match frame.symbol() {
                Some(symbol) => println!(
                    "{:#016x} - {} + {:#x}",
                    frame.ip(),
                    symbol.name(),
                    symbol.offset(),
                ),
                None => println!("{:#016x} - ???", frame.ip()),
            }
        }
    }
    println!();
    Ok(())
}

/// Process diagnostics-related CLI commands.
pub fn process_diagnostics_cli(
    cli: &MayastorCliArgs,
) -> Option<Result<(), Box<dyn std::error::Error>>> {
    match cli.diagnose_stack {
        None => None,
        Some(pid) => Some(collect_process_stack(pid)),
    }
}

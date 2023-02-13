use std::{
    ffi::{OsStr, OsString},
    io::{BufRead, BufReader, Write},
    os::unix::ffi::OsStringExt,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    thread::JoinHandle,
};

/// Runs a shell command and returns its output.
pub fn run_command_args<P, A, S>(
    path: P,
    args: A,
    short_desc: Option<&str>,
) -> Result<(ExitStatus, Vec<OsString>), String>
where
    P: AsRef<Path>,
    A: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let desc = path.as_ref().as_os_str().to_str().unwrap();
    let mut cmd = Command::new(path.as_ref().as_os_str());
    cmd.args(args);

    run_command(&mut cmd, desc, short_desc)
}

/// Runs a shell command and returns its output.
pub fn run_command(
    cmd: &mut Command,
    desc: &str,
    short_desc: Option<&str>,
) -> Result<(ExitStatus, Vec<OsString>), String> {
    let (mut child, out_reader) = spawn_child(cmd, desc, short_desc)?;

    let status = match child.wait() {
        Ok(s) => s,
        Err(e) => {
            return Err(format!(
                "Failed to wait on spawned child process '{desc}': {e}",
            ));
        }
    };

    let lines = match out_reader.join() {
        Ok(lines) => lines,
        Err(e) => {
            return Err(format!(
                "Failed to read output of child process  '{desc}': {e:?}",
            ));
        }
    };

    Ok((status, lines))
}

/// Spawns a child process and returns a thread handle that reads its standard
/// output.
fn spawn_child(
    cmd: &mut Command,
    desc: &str,
    short_desc: Option<&str>,
) -> Result<(Child, JoinHandle<Vec<OsString>>), String> {
    match cmd.stdout(Stdio::piped()).spawn() {
        Ok(mut child) => {
            let short_desc = short_desc.map(String::from);

            let stddout = BufReader::new(child.stdout.take().unwrap());

            let out_reader = thread::spawn(move || {
                let mut lines = Vec::new();
                for line in stddout.split(b'\n').filter_map(|l| l.ok()) {
                    if let Some(ref s) = short_desc {
                        print!("    [{s}] ");
                        std::io::stdout().write_all(&line).unwrap();
                        println!();
                    }

                    lines.push(OsString::from_vec(line));
                }
                lines
            });

            Ok((child, out_reader))
        }
        Err(e) if e.kind() == ::std::io::ErrorKind::NotFound => {
            Err(format!("Command '{desc}' not found: {e}"))
        }
        Err(e) => Err(format!("Failed to spawn a child process '{desc}': {e}")),
    }
}

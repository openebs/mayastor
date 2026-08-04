import asyncio
from collections import namedtuple
import asyncssh
import subprocess

CommandReturn = namedtuple("CommandReturn", "returncode stdout stderr")


def run_cmd(cmd, check=True):
    subprocess.run(cmd, shell=True, check=check)


def losetup_disk(file, size):
    """Create a sparse file of the given size and attach it as a loop device."""
    run_cmd(f"rm -f '{file}'", True)
    run_cmd(f"truncate -s {size} '{file}'", True)
    out = subprocess.run(
        f"sudo -E losetup -f '{file}' --show",
        shell=True,
        check=True,
        capture_output=True,
    )
    return out.stdout.decode("ascii").strip("\n")


def losetup_detach(disk, file):
    """Detach a loop device made by losetup_disk and remove its file."""
    run_cmd(f"sudo -E losetup -d {disk}", True)
    run_cmd(f"rm -f '{file}'", True)


async def run_cmd_async(cmd):
    """Runs a command on the current machine."""
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()

    output_message = f"\n[{proc.pid}] Command:\n{cmd}"
    # Append stdout/stderr to the output message
    if stdout.decode() != "":
        output_message += f"\n[{proc.pid}] stdout:\n{stdout.decode()}"
    if stderr.decode() != "":
        output_message += f"\n[{proc.pid}] stderr:\n{stderr.decode()}"

    # If a non-zero return code was thrown, raise an exception
    if proc.returncode != 0:
        output_message += f"\nReturned error code: {proc.returncode}"

        if stderr.decode() != "":
            output_message += f"\nstderr:\n{stderr.decode()}"
        raise ChildProcessError(output_message)

    return CommandReturn(proc.returncode, stdout.decode(), stderr.decode())


async def run_cmd_async_at(host, cmd):
    """Runs the command async at the given host."""
    async with asyncssh.connect(host) as conn:
        result = await conn.run(cmd, check=False)

        output_message = f"Command: {host}:{cmd}\n"
        # Append stdout/stderr to the output message
        if result.stdout != "":
            output_message += f"\nstdout:\n{result.stdout}"
        if result.stderr != "":
            output_message += f"\nstderr:\n{result.stderr}"

        if result.exit_status != 0:

            output_message += f"\nReturned error code: {result.exit_status}"

            if result.stderr != "":
                output_message += f"\nstderr:\n{result.stderr}"
            raise ChildProcessError(output_message)

        return CommandReturn(result.exit_status, result.stdout, result.stderr)

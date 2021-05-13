import asyncio
from collections import namedtuple

CommandReturn = namedtuple("CommandReturn", "returncode stdout stderr")


async def run_cmd_async(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()

    output_message = f"\n[{proc.pid}] Command:\n{cmd}"
    # Append stdout/stderr to the output message
    if stdout.decode() != "":
        output_message += f"\n[{proc.pid}] stdout:\n{stdout.decode()}"
    if stderr.decode() != "":
        output_message += f"\n[{proc.pid}] stderr:\n{stderr.decode()}"

    # If a non-zero return code was thrown, raise an exception
    if proc.returncode != 0:
        output_message += \
            f"\nReturned error code: {proc.returncode}"

        if stderr.decode() != "":
            output_message += \
                f"\nstderr:\n{stderr.decode()}"
        raise ChildProcessError(output_message)

    return CommandReturn(
        proc.returncode,
        stdout.decode(),
        stderr.decode())

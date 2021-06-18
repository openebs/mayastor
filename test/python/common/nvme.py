from urllib.parse import urlparse
import subprocess
import time
import json
from common.command import run_cmd_async_at


async def nvme_remote_connect_all(remote, host, port):
    command = f"sudo nvme connect-all -t tcp -s {port} -a {host}"
    await run_cmd_async_at(remote, command)


async def nvme_remote_connect(remote, uri):
    """Connect to the remote nvmf target on this host."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    nqn = u.path[1:]

    command = "sudo nvme connect -t tcp -s {0} -a {1} -n {2}".format(
        port, host, nqn)

    await run_cmd_async_at(remote, command)
    time.sleep(1)
    command = "sudo nvme list -v -o json"

    discover = await run_cmd_async_at(remote, command)
    discover = json.loads(discover.stdout)

    dev = list(
        filter(lambda d: nqn in d.get("SubsystemNQN"),
               discover.get("Devices")))

    # we should only have one connection
    assert len(dev) == 1
    dev_path = dev[0].get('Controllers')[0].get('Namespaces')[0].get(
        'NameSpace')

    return f"/dev/{dev_path}"


async def nvme_remote_disconnect(remote, uri):
    """Disconnect the given URI on this host."""
    u = urlparse(uri)
    nqn = u.path[1:]

    command = "sudo nvme disconnect -n {0}".format(nqn)
    await run_cmd_async_at(remote, command)


async def nvme_remote_discover(remote, uri):
    """Discover target."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname

    command = "sudo nvme discover -t tcp -s {0} -a {1}".format(port, host)
    output = await run_cmd_async_at(remote, command).stdout
    if not u.path[1:] in str(output.stdout):
        raise ValueError("uri {} is not discovered".format(u.path[1:]))


def nvme_connect(uri):
    u = urlparse(uri)
    port = u.port
    host = u.hostname
    nqn = u.path[1:]

    command = "sudo nvme connect -t tcp -s {0} -a {1} -n {2}".format(
        port, host, nqn)
    subprocess.run(command, check=True, shell=True, capture_output=False)
    time.sleep(1)
    command = "sudo nvme list -v -o json"
    discover = json.loads(
        subprocess.run(command,
                       shell=True,
                       check=True,
                       text=True,
                       capture_output=True).stdout)

    dev = list(
        filter(lambda d: nqn in d.get("SubsystemNQN"),
               discover.get("Devices")))

    # we should only have one connection
    assert len(dev) == 1
    device = "/dev/{}".format(dev[0].get('Namespaces')[0].get('NameSpace'))
    return device


def nvme_discover(uri):
    """Discover target."""
    u = urlparse(uri)
    port = u.port
    host = u.hostname

    command = "sudo nvme discover -t tcp -s {0} -a {1}".format(port, host)
    output = subprocess.run(command,
                            check=True,
                            shell=True,
                            capture_output=True,
                            encoding="utf-8")
    if not u.path[1:] in str(output.stdout):
        raise ValueError("uri {} is not discovered".format(u.path[1:]))


def nvme_disconnect(uri):
    """Disconnect the given URI on this host."""
    u = urlparse(uri)
    nqn = u.path[1:]

    command = "sudo nvme disconnect -n {0}".format(nqn)
    subprocess.run(command, check=True, shell=True, capture_output=True)

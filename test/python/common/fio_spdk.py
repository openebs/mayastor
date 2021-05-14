import asyncio
import os
import shutil
from common.command import run_cmd_async
from urllib.parse import urlparse

class FioSpdk(object):

    def __init__(self, name, rw, uri, runtime=15):
        self.name = name
        self.rw = rw
        u = urlparse(uri)
        self.host = u.hostname
        self.port = u.port
        self.nqn = u.path[1:]
        self.cmd = shutil.which("fio")
        self.runtime = runtime

    async def run(self):
        spdk_path = os.environ.get('SPDK_PATH')
        if spdk_path == None:
            spdk_path = os.getcwd() + '/../../spdk-sys/spdk/build'
        command = ("sudo LD_PRELOAD={}/fio/spdk_nvme fio --ioengine=spdk "
                   "--direct=1 --bs=4k --time_based=1 --runtime=15 "
                   "--thread=1 --rw={} --group_reporting=1 --norandommap=1 "
                   "--iodepth=64 --name={} --filename=\'trtype=tcp "
                   "adrfam=IPv4 traddr={} trsvcid={} subnqn={} ns=1\'").format(
            spdk_path, self.rw, self.name, self.host, self.port,
            self.nqn.replace(":", "\\:"))

        await asyncio.wait_for(run_cmd_async(command), self.runtime + 5)

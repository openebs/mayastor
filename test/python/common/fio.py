import shutil
from common.command import run_cmd_async
import asyncio


class Fio(object):

    def __init__(self, name, rw, device, runtime=15):
        self.name = name
        self.rw = rw
        self.device = device
        self.cmd = shutil.which("fio")
        self.output = {}
        self.success = {}
        self.runtime = runtime

    def build(self) -> str:
        command = ("sudo fio --ioengine=linuxaio --direct=1 --bs=4k "
                   "--time_based=1 --rw={} "
                   "--group_reporting=1 --norandommap=1 --iodepth=64 "
                   "--runtime={} --name={} --filename={}").format(
            self.rw, self.runtime, self.name, self.device)

        return command

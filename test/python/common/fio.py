import shutil


class Fio(object):
    def __init__(self, name, rw, device, runtime=15, optstr=None):
        self.name = name
        self.rw = rw
        self.device = device
        self.cmd = shutil.which("fio")
        self.output = {}
        self.success = {}
        self.runtime = runtime
        self.optstr = optstr

    def build(self) -> str:
        if isinstance(self.device, str):
            devs = [self.device]
        else:
            devs = self.device

        command = (
            "sudo fio --ioengine=linuxaio --direct=1 --bs=4k "
            "--time_based=1  {} --rw={} "
            "--group_reporting=1 --norandommap=1 --iodepth=64 "
            "--runtime={} --name={}"
        ).format(self.optstr, self.rw, self.runtime, self.name)

        command += " --filename=".join(map(str, devs))

        return command

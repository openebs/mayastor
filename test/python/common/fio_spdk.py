import os
import shutil
from urllib.parse import urlparse


class FioSpdk(object):
    def __init__(self, name, rw, uris, runtime=15):
        self.name = name
        self.rw = rw
        if isinstance(uris, str):
            uris = [uris]

        self.filenames = []
        for uri in uris:
            u = urlparse(uri)
            self.filenames.append(
                (
                    "'trtype=tcp adrfam=IPv4 traddr={} " "trsvcid={} subnqn={} ns=1'"
                ).format(u.hostname, u.port, u.path[1:].replace(":", "\\:"))
            )

        self.cmd = shutil.which("fio")
        self.runtime = runtime

    def build(self) -> str:
        spdk_path = os.environ.get("SPDK_PATH")
        if spdk_path is None:
            spdk_path = os.getcwd() + "/../../spdk-rs/spdk/build"
        command = (
            "sudo LD_PRELOAD={}/fio/spdk_nvme fio --ioengine=spdk "
            "--direct=1 --bs=4k --time_based=1 --runtime={} "
            "--thread=1 --rw={} --group_reporting=1 --norandommap=1 "
            "--iodepth=64 --name={} --filename={}"
        ).format(
            spdk_path,
            self.runtime,
            self.rw,
            self.name,
            " --filename=".join(map(str, self.filenames)),
        )

        return command

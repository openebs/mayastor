import os
import subprocess
import json
from common.util import mayastor_target_dir


class MayastorClient:
    """Abstraction around Mayastor command line client, which allows
    flexible client configuration and supports parsing of command output
    depending on requested output mode.
    In order to invoke mayastor CLI just perform a call operation against
    a MayastorClient instance, passing all the commands via function arguments:
        mscli = get_msclient()
        mscli("controller", "list")
    """

    def __init__(self, path, cfg):
        assert path, "CLI command path must be provided"
        self.path = path
        self.verbose = cfg.get("verbose", False)
        self.url = cfg.get("url", None)
        self.quiet = cfg.get("quiet", False)
        self.output = cfg.get("output", "default")

    def __call__(self, *args):
        cmd = [self.path, "-o", self.output]

        if self.quiet:
            cmd.append("-q")
        if self.verbose:
            cmd.append("-v")
        if self.url:
            cmd += ["-b", self.url]

        cmd += list(args)

        output = subprocess.check_output(cmd, shell=False)
        output = output.decode("utf-8")

        if self.output == "json":
            output = json.loads(output)

        return output

    def with_url(self, url):
        """Set URL of the Mayastor."""
        self.url = url
        return self

    def with_json_output(self):
        """Set JSON output mode."""
        self.output = "json"
        return self

    def with_default_output(self):
        """Set default output mode (non-JSON)."""
        self.output = "default"
        return self

    def with_verbose(self, verbose):
        """Request verbose output."""
        self.verbose = verbose
        return self


def get_msclient(cfg=None):
    """Instantiate mayastor CLI object based on user config."""
    # Check that CLI binary exists.
    p = "%s/io-engine-client" % mayastor_target_dir()
    if not os.path.isfile(p):
        raise FileNotFoundError("No Mayastor CLI command available")

    return MayastorClient(p, {} if cfg is None else cfg)

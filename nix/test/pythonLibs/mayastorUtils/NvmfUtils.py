def subsystem_is_discoverable(host, ip, port, subsys):
    discoveryResponse = host.succeed("nvme discover -a " + ip + " -t tcp -s " + port)
    return subsys in discoveryResponse

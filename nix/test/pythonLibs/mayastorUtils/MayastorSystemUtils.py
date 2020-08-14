DEFAULT_NEXUS_PORT = "8420"
DEFAULT_REPLICA_PORT = "8430"

def wait_for_mayastor_all(machines):
    for node in machines:
        node.wait_for_unit("multi-user.target")
        node.wait_for_open_port(10124)

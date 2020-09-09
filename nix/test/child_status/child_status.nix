{ pkgs, lib, ... }:
let
  node_ip = "192.168.0.1";
  nexus_uuid = "19b98ac8-c1ea-11ea-8e3b-d74f5d324a22";
  child_1 = "malloc:///malloc0?blk_size=512&size_mb=20";
  child_2 = "malloc:///malloc1?blk_size=512&size_mb=20";
  common = import ../common.nix { inherit pkgs; };
in
{
  name = "child_status";

  nodes = {
    node = common.defaultMayastorNode { ip = node_ip; };
  };

  testScript = ''
    ${common.importMayastorUtils}

    from time import sleep


    def init():
        start_all()
        mayastorUtils.wait_for_mayastor_all(machines)
        node.succeed(
            "mayastor-client -a ${node_ip} nexus create ${nexus_uuid} 20MiB '${child_1}'"
        )


    def get_nexus_state():
        result = node.succeed("mayastor-client -a ${node_ip} nexus list").split()
        return result[7]


    def check_nexus_state(expected_state):
        state = get_nexus_state()
        assert state == expected_state, "Nexus state {}, expected, {}".format(
            state, expected_state
        )


    def get_children_states():
        result = node.succeed(
            "mayastor-client -a ${node_ip} nexus children ${nexus_uuid}"
        ).split()
        child1_state = result[3]
        child2_state = result[5]
        return [child1_state, child2_state]


    def check_children_states(child1_expected_state, child2_expected_state):
        states = get_children_states()
        assert states[0] == child1_expected_state, "Child 1 state {}, expected {}".format(
            states[0], child1_expected_state
        )
        assert states[1] == child2_expected_state, "Child 2 state {}, expected {}".format(
            states[1], child2_expected_state
        )


    init()

    with subtest("rebuild on mayastor restart"):
        node.succeed(
            "mayastor-client -a ${node_ip} nexus add ${nexus_uuid} '${child_2}' true"
        )
        check_nexus_state("degraded")
        check_children_states("online", "degraded")

        # Restart mayastor service
        node.systemctl("restart mayastor")
        sleep(1)

        # Rebuild should have been completed and everything should be healthy
        check_nexus_state("online")
        check_children_states("online", "online")

    with subtest("fault child"):
        node.succeed(
            "mayastor-client -a ${node_ip} nexus child fault ${nexus_uuid} '${child_2}'"
        )
        check_nexus_state("degraded")
        check_children_states("online", "faulted")

        # Restart mayastor service
        node.systemctl("restart mayastor")
        sleep(1)

        # The faulted child should remain faulted causing the nexus to be in a degraded state
        check_nexus_state("degraded")
        check_children_states("online", "faulted")
  '';
}

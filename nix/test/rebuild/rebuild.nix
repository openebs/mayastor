{ pkgs, lib, ... }:
let
  node1ip = "192.168.0.1";
  node2ip = "192.168.0.2";
  nexus_uuid = "19b98ac8-c1ea-11ea-8e3b-d74f5d324a22";
  replica1_uuid = "9a9843db-f715-4f52-8aa4-119f5df3d05d";
  replica2_uuid = "9a9843db-f715-4f52-8aa4-119f5df3d06e";
in
{
  name = "rebuild";
  meta = with pkgs.stdenv.lib.maintainers; {
    maintainers = [ paulyoong ];
  };

  nodes = {
    node1 = { config, lib, ... }: {

      virtualisation = {
        memorySize = 2048;
        emptyDiskImages = [ 512 ];
        vlans = [ 1 ];
      };

      boot = {
        kernel.sysctl = {
          "vm.nr_hugepages" = 512;
        };
      };

      networking.firewall.enable = false;
      networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkOverride 0 [
        { address = node1ip; prefixLength = 24; }
      ];

      environment = {
        systemPackages = with pkgs; [
          images.mayastor-develop
        ];

        etc."mayastor-config.yaml" = {
          mode = "0664";
          source = ./node1-mayastor-config.yaml;
        };
      };

      systemd.services.mayastor = {
        enable = true;
        wantedBy = [ "multi-user.target" ];
        after = [ "network.target" ];
        description = "Mayastor";
        environment = {
          MY_POD_IP = node1ip;
        };

        serviceConfig = {
          ExecStart = "${pkgs.images.mayastor-develop}/bin/mayastor -g 0.0.0.0:10124 -y /etc/mayastor-config.yaml";
        };
      };
    };
    node2 = { config, lib, ... }: {

      virtualisation = {
        memorySize = 2048;
        emptyDiskImages = [ 512 ];
        vlans = [ 1 ];
      };

      boot = {
        kernel.sysctl = {
          "vm.nr_hugepages" = 512;
        };
      };

      networking.firewall.enable = false;
      networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkOverride 0 [
        { address = node2ip; prefixLength = 24; }
      ];

      environment = {
        systemPackages = with pkgs; [
          images.mayastor-develop
        ];

        etc."mayastor-config.yaml" = {
          mode = "0664";
          source = ./node2-mayastor-config.yaml;
        };
      };

      systemd.services.mayastor = {
        enable = true;
        wantedBy = [ "multi-user.target" ];
        after = [ "network.target" ];
        description = "Mayastor";
        environment = {
          MY_POD_IP = node2ip;
        };

        serviceConfig = {
          ExecStart = "${pkgs.images.mayastor-develop}/bin/mayastor -g 0.0.0.0:10124 -y /etc/mayastor-config.yaml";
        };
      };
    };
  };

  testScript = ''
    from time import sleep


    def get_replica_states():
        result = node1.succeed(
            "mayastor-client -a ${node1ip} nexus children ${nexus_uuid}"
        ).split()
        replica1_state = result[3]
        replica2_state = result[5]
        return [replica1_state, replica2_state]


    def get_replica1_uri():
        result = node1.succeed("mayastor-client -a ${node1ip} replica list")
        return "bdev:///" + result.split("bdev:///")[1]


    def get_replica2_uri():
        result = node2.succeed("mayastor-client -a ${node2ip} replica list")
        print(result)
        return "nvmf://" + result.split("nvmf://")[1]


    def get_num_rebuilds():
        result = node1.succeed("mayastor-client -a ${node1ip} nexus list").split()
        # Number of rebuilds is the last entry
        return int(result[len(result) - 1])


    def get_rebuild_progress_percentage():
        progress = exec_rebuild_operation("progress", get_replica2_uri()).split("%")[0]
        return int(progress)


    def exec_rebuild_operation(operation, child_uri):
        return node1.succeed(
            "mayastor-client -a ${node1ip} rebuild {} ${nexus_uuid} '{}'".format(
                operation, child_uri
            )
        )


    def get_rebuild_state(child_uri):
        return exec_rebuild_operation("state", child_uri).strip().lower()


    def startup():
        start_all()
        node1.wait_for_unit("multi-user.target")
        node2.wait_for_unit("multi-user.target")

        # Create replicas on nodes
        node1.succeed(
            "mayastor-client -a ${node1ip} replica create pool1 ${replica1_uuid} --size 100MiB --protocol none"
        )
        node2.succeed(
            "mayastor-client -a ${node2ip} replica create pool2 ${replica2_uuid} --size 100MiB --protocol nvmf"
        )

        # Create nexus on node 1
        node1.succeed(
            "mayastor-client -a ${node1ip} nexus create ${nexus_uuid} 100MiB '{}'".format(
                get_replica1_uri()
            )
        )
        # Add a second child to nexus and don't start rebuilding
        node1.succeed(
            "mayastor-client -a ${node1ip} nexus add ${nexus_uuid} '{}' true".format(
                get_replica2_uri()
            )
        )


    def teardown():
        # Destroy nexus
        node1.succeed(
            "mayastor-client -a ${node1ip} nexus destroy ${nexus_uuid}"
        )
        # Delete replicas
        node1.succeed(
            "mayastor-client -a ${node1ip} replica destroy ${replica1_uuid}"
        )
        node2.succeed(
            "mayastor-client -a ${node2ip} replica destroy ${replica2_uuid}"
        )

        node1.shutdown()
        node2.shutdown()


    def new_env():
        teardown()
        startup()


    """
    Test cases
    """

    startup()
    replica2_uri = get_replica2_uri()

    with subtest("start a rebuild"):
        exec_rebuild_operation("start", replica2_uri)
        rebuild_state = get_rebuild_state(replica2_uri)
        expected_state = "running"
        assert rebuild_state == expected_state, "Rebuild state {}, expected {}".format(
            rebuild_state, expected_state
        )

    with subtest("pause a rebuild"):
        exec_rebuild_operation("pause", replica2_uri)

        # Wait for the rebuild to pause.
        # We retry because this may take a little time (the rebuild must complete outstanding I/Os before becoming paused).
        retries = 5
        while get_rebuild_state(replica2_uri) != "paused" and retries > 0:
            sleep(1)
            retries -= 1

        rebuild_state = get_rebuild_state(replica2_uri)
        expected_state = "paused"
        assert rebuild_state == expected_state, "Rebuild state {}, expected {}".format(
            rebuild_state, expected_state
        )

    with subtest("resume a rebuild"):
        exec_rebuild_operation("resume", replica2_uri)
        rebuild_state = get_rebuild_state(replica2_uri)
        expected_state = "running"
        assert rebuild_state == expected_state, "Rebuild state {}, expected {}".format(
            rebuild_state, expected_state
        )

    with subtest("get number of rebuilds"):
        num_rebuilds = get_num_rebuilds()
        expected_num_rebuilds = 1
        assert (
            num_rebuilds == expected_num_rebuilds
        ), "Number of rebuilds {}, expected {}".format(num_rebuilds, expected_num_rebuilds)

    with subtest("network fault"):
        # Wait for some rebuild progress
        while get_rebuild_progress_percentage() < 5:
            sleep(1)

        # Create a transient network fault on the rebuild source.
        # We sleep between blocking and unblocking to ensure the
        # network fault has time to take effect.
        node1.block()
        sleep(2)
        node1.unblock()

        # Wait for the rebuild job to terminate.
        # We retry a number of times as this may take a while.
        retries = 5
        while get_num_rebuilds() > 0 and retries > 0:
            sleep(1)
            retries -= 1

        # Expect the rebuild job to have been terminated
        num_rebuilds = get_num_rebuilds()
        expected_num_rebuilds = 0
        assert (
            num_rebuilds == expected_num_rebuilds
        ), "Number of rebuilds {}, expected {}".format(num_rebuilds, expected_num_rebuilds)

        states = get_replica_states()
        expected_replica1_state = "online"
        assert (
            states[0] == expected_replica1_state
        ), "Replica 1 has state {}, expected {}".format(states[0], expected_replica1_state)

        expected_replica2_state = "faulted"
        assert (
            states[1] == expected_replica2_state
        ), "Replica 2 has state {}, expected {}".format(states[1], expected_replica2_state)

    # Create a fresh environment for the subsequent tests
    new_env()
    replica2_uri = get_replica2_uri()

    with subtest("crash rebuild destination node"):
        # Start rebuild
        exec_rebuild_operation("start", replica2_uri)
        rebuild_state = get_rebuild_state(replica2_uri)
        expected_state = "running"
        assert rebuild_state == expected_state, "Rebuild state {}, expected {}".format(
            rebuild_state, expected_state
        )

        # Wait for some rebuild progress
        while get_rebuild_progress_percentage() < 5:
            sleep(1)

        # Crash and restart destination node
        node2.crash()
        node2.start()
        node2.wait_for_unit("multi-user.target")

        # Wait for the rebuild job to terminate.
        # We retry a number of times as this may take a while.
        retries = 5
        while get_num_rebuilds() > 0 and retries > 0:
            sleep(1)
            retries -= 1

        num_rebuilds = get_num_rebuilds()
        expected_num_rebuilds = 0
        assert (
            num_rebuilds == expected_num_rebuilds
        ), "Number of rebuilds {}, expected {}".format(num_rebuilds, expected_num_rebuilds)

        states = get_replica_states()
        expected_replica1_state = "online"
        assert (
            states[0] == expected_replica1_state
        ), "Replica 1 has state {}, expected {}".format(states[0], expected_replica1_state)

        expected_replica2_state = "faulted"
        assert (
            states[1] == expected_replica2_state
        ), "Replica 2 has state {}, expected {}".format(states[1], expected_replica2_state)
  '';
}

{ pkgs, lib, ... }:
let
  backendIp = "192.168.0.1";
  targetIp = "192.168.0.2";
  initiatorIp = "192.168.0.3";
  common = import ../common.nix { inherit pkgs; };
in
{
  name = "nvmf_against_replica_and_nexus_ports";
  meta = with pkgs.stdenv.lib.maintainers; {
    maintainers = [ tjoshum ];
  };

  nodes = {
    backend = common.defaultMayastorNode { ip = backendIp; };
    target = common.defaultMayastorNode { ip = targetIp; };
    initiator = common.defaultMayastorNode { ip = initiatorIp; };
  };

  testScript = ''
    ${common.importMayastorUtils}

    start_all()
    mayastorUtils.wait_for_mayastor_all(machines)

    replicaId = "5b5b04ea-c1e3-11ea-bd82-a7d5cb04b391"
    print(backend.succeed("mayastor-client pool create pool1 /dev/vdb"))
    print(
        backend.succeed(
            "mayastor-client replica create --protocol nvmf --size 64MiB pool1 " + replicaId
        )
    )

    with subtest("discover replica over replica port"):
        assert mayastorUtils.subsystem_is_discoverable(
            backend, "${backendIp}", mayastorUtils.DEFAULT_REPLICA_PORT, replicaId
        )

    with subtest("discover replica over nexus port"):
        assert mayastorUtils.subsystem_is_discoverable(
            backend, "${backendIp}", mayastorUtils.DEFAULT_NEXUS_PORT, replicaId
        )
  '';
}

{ pkgs, lib, ... }:
let
  backendIp = "192.168.0.1";
  targetIp = "192.168.0.2";
  initiatorIp = "192.168.0.3";
  common = import ../common.nix { inherit pkgs; };
in
{
  name = "fio_against_nvmf_nexus_with_replica";
  meta = with pkgs.stdenv.lib.maintainers; {
    maintainers = [ tjoshum ];
  };

  nodes = {
    backend = common.defaultMayastorNode {};
    target = common.defaultMayastorNode { ip = targetIp; };
    initiator = common.defaultMayastorNode { ip = initiatorIp; };
  };

  testScript = ''
    ${common.importMayastorUtils}

    start_all()
    mayastorUtils.wait_for_mayastor_all(machines)

    replicaId = "5b5b04ea-c1e3-11ea-bd82-a7d5cb04b391"
    with subtest("setup replica"):
        print(backend.succeed("mayastor-client pool create pool1 /dev/vdb"))
        print(
            backend.succeed(
                "mayastor-client replica create --protocol nvmf --size 64MiB pool1 "
                + replicaId
            )
        )

    with subtest("connect nexus to replica"):
        print(
            target.succeed(
                "mayastor-client nexus create 19b98ac8-c1ea-11ea-8e3b-d74f5d324a22 64MiB nvmf://${backendIp}:"
                + mayastorUtils.DEFAULT_REPLICA_PORT
                + "/nqn.2019-05.io.openebs:"
                + replicaId
            )
        )
        print(
            target.succeed(
                "mayastor-client nexus publish -p nvmf 19b98ac8-c1ea-11ea-8e3b-d74f5d324a22"
            )
        )

    with subtest("should be able to connect to the target"):
        print(
            initiator.succeed(
                "nvme connect-all -a ${targetIp} -t tcp -s "
                + mayastorUtils.DEFAULT_NEXUS_PORT
            )
        )

    with subtest("should be able to run FIO with verify=crc32"):
        print(
            initiator.succeed(
                "fio --thread=1 --ioengine=libaio --direct=1 --bs=4k --iodepth=1 --rw=randrw --verify=crc32 --numjobs=1 --group_reporting=1 --runtime=15 --name=job --filename="
                + "/dev/nvme0n1"
            )
        )

    with subtest("should be able to disconnect from the target"):
        print(initiator.succeed("nvme disconnect-all"))
  '';
}

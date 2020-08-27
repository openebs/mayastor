{ pkgs, lib, ... }:
let
  targetIp = "192.168.0.1";
  initiatorIp = "192.168.0.2";
  common = import ../common.nix { inherit pkgs; };
in
{
  name = "fio_against_nvmf_target";

  nodes = {
    target = common.defaultMayastorNode { ip = targetIp; mayatorConfigYaml = ./mayastor-config.yaml; };
    initiator = common.defaultMayastorNode { ip = initiatorIp; };
  };

  testScript = ''
    ${common.importMayastorUtils}

    from time import sleep

    start_all()
    mayastorUtils.wait_for_mayastor_all(machines)

    with subtest("the bdev of the target should be listed"):
        print(target.succeed("mayastor-client -a ${targetIp} bdev list"))

    with subtest("should be able to discover the target"):
        print(initiator.succeed("nvme discover -a ${targetIp} -t tcp -s 8420"))

    with subtest("should be able to connect to the target"):
        print(initiator.succeed("nvme connect-all -a ${targetIp} -t tcp -s 8420"))

    # First verify IO passes over the connection
    with subtest("should be able to run FIO with verify=crc32"):
        print(
            initiator.succeed(
                "fio --thread=1 --ioengine=libaio --direct=1 --bs=4k --iodepth=1 --rw=randrw --verify=crc32 --numjobs=1 --group_reporting=1 --runtime=15 --name=job --filename="
                + "/dev/nvme0n1"
            )
        )

    # Create a network fault to the target and expect fio to hang
    with subtest("FIO should fail to complete due to the network disconnection"):
        # break the network connection
        target.block()
        print(
            initiator.fail(
                "timeout -k 1s 60s fio --thread=1 --ioengine=libaio --direct=1 --bs=4k --iodepth=1 --rw=randrw --verify=crc32 --numjobs=1 --group_reporting=1 --runtime=15 --name=job --filename="
                + "/dev/nvme0n1"
            )
        )
        # reconnect the network to allow nvme disconnection
        target.unblock()

    with subtest("should be able to disconnect from the target"):
        print(initiator.succeed("nvme disconnect-all"))
  '';
}

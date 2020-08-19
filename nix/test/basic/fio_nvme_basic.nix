{ pkgs, lib, ... }:
let
  targetIp = "192.168.0.1";
  initiatorIp = "192.168.0.2";
in
{
  name = "fio_against_nvmf_target";
  meta = with pkgs.stdenv.lib.maintainers; {
    maintainers = [ gila ];
  };

  nodes = {
    target = { config, lib, ... }: {

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
        { address = targetIp; prefixLength = 24; }
      ];

      environment = {
        systemPackages = with pkgs; [
          mayastor
        ];

        etc."mayastor-config.yaml" = {
          mode = "0664";
          source = ./mayastor-config.yaml;
        };
      };

      systemd.services.mayastor = {
        enable = true;
        wantedBy = [ "multi-user.target" ];
        after = [ "network.target" ];
        description = "Mayastor";
        environment = {
          MY_POD_IP = targetIp;
        };

        serviceConfig = {
          ExecStart = "${pkgs.mayastor}/bin/mayastor -g 0.0.0.0:10124 -y /etc/mayastor-config.yaml";
        };
      };
    };

    initiator = { config, lib, ... }:
      {
        virtualisation = {
          memorySize = 2048;
          emptyDiskImages = [ 512 ];
          vlans = [ 1 ];
        };

        networking.firewall.enable = false;
        networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkOverride 0 [
          { address = initiatorIp; prefixLength = 24; }
        ];
        boot = {
          kernelModules = [
            "nvme-tcp"
          ];
        };

        environment = {
          systemPackages = with pkgs; [
            nvme-cli
            fio
          ];
        };
      };
  };

  testScript = ''
    start_all()
    target.wait_for_unit("multi-user.target")
    initiator.wait_for_unit("multi-user.target")

    with subtest("the bdev of the target should be listed"):
        print(target.succeed("mayastor-client -a ${targetIp} bdev list"))

    with subtest("should be able to discover the target"):
        print(initiator.succeed("nvme discover -a ${targetIp} -t tcp -s 8420"))

    with subtest("should be able to connect to the target"):
        print(initiator.succeed("nvme connect-all -a ${targetIp} -t tcp -s 8420"))

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

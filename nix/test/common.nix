{ pkgs, ... }:
{
    importMayastorUtils = ''
      import sys
      
      sys.path.insert(0, "${./pythonLibs}")
      import mayastorUtils
    '';

    # We provide sensible defaults for these fields, so that tests can be decluttered.
    # TODO Find a way to have the default IP just be DHCP
    defaultMayastorNode = {
      ip ? "192.168.0.1",
      mayatorConfigYaml ? ./default-mayastor-config.yaml
    }: { config, lib, ... }: {

      virtualisation = {
        memorySize = 4096;
        emptyDiskImages = [ 512 ];
        vlans = [ 1 ];
      };

      boot = {
        kernel.sysctl = {
          "vm.nr_hugepages" = 512;
        };
        kernelModules = [
          "nvme-tcp"
        ];
      };

      networking.firewall.enable = false;
      networking.interfaces.eth1.ipv4.addresses = pkgs.lib.mkOverride 0 [
        { address = ip; prefixLength = 24; }
      ];

      environment = {
        systemPackages = with pkgs; [
          mayastor
          nvme-cli
          fio
        ];

        etc."mayastor-config.yaml" = {
          mode = "0664";
          source = mayatorConfigYaml;
        };
      };

      systemd.services.mayastor = {
        enable = true;
        wantedBy = [ "multi-user.target" ];
        after = [ "network.target" ];
        description = "Mayastor";
        environment = {
          MY_POD_IP = ip;
        };

        serviceConfig = {
          ExecStart = "${pkgs.mayastor}/bin/mayastor -g 0.0.0.0:10124 -y /etc/mayastor-config.yaml";
        };
      };
    };
}

# Run me with 'nix-build nix/tests/basic-mayastor.nix'

import <nixpkgs/nixos/tests/make-test-python.nix> ({ pkgs, version ? 4, ... }:

let
  mayastornode =
    { pkgs, ... }:
    { 
      virtualisation = {
        # Probably not a problem for our tests, but many nixos test use 2047
        # (seemingly due to qemu-system-i386's 2047M memory limit).
        # Maybe it's a idiom, or maybe I'm just a monkey on a ladder.
        memorySize = 2047;
        emptyDiskImages = [ 10240 10240 ]; # 2 x 10G data disks
      };

      networking.firewall.enable = false; # One day, we'll only open what we need

      # boot.kernelModules = [ "nvme_tcp" ]; # Do we need this?
    };


  # TOMTODO Rename
  tm_pkgs = import ../../default.nix;
  mayastor-develop = tm_pkgs.mayastor.override { release = false; };

  commonTestHeader =
    ''
      # Import the mayastor utils.
      # TOMTODO We should be using something like pythonPackages.buildPythonPackage.
      import importlib.util
      spec = importlib.util.spec_from_file_location("mylib", "${./mylib.py}")
      mylib = importlib.util.module_from_spec(spec)
      spec.loader.exec_module(mylib)

      mylib.my_function() # TOMTODO Unused for now, just here as an example.

      start_all()

      global machines
      with log.nested("starting mayastor instances"):
        for machine in machines:
            machine.copy_from_host(
                "${ mayastor-develop }/bin/mayastor",
                "/mnt/mayastor",
            )
            machine.execute("/mnt/mayastor -g 127.0.0.1:10124 &") # TOMTODO Leave as default.

      node1.wait_for_open_port(10124)
    '';
in

{
  name = "bringup";
  meta = with pkgs.stdenv.lib.maintainers; {
    maintainers = [ tjoshum ];
  };
  skipLint = true; # TOMTODO Remove one day

  nodes =
    { node1 = mayastornode;
      node2 = mayastornode;
    };

  testScript =
    ''
      ${ commonTestHeader }

      print(node1.succeed("${ mayastor-develop }/bin/mayastor-client pool create pool1 /dev/vdb"))

      print(
        node1.succeed(
          "${ mayastor-develop }/bin/mayastor-client replica create --protocol nvmf --size 1GB pool1 5b5b04ea-c1e3-11ea-bd82-a7d5cb04b391"))

      """
      # Test that the replica can be discovered.
      node1.succeed("nix-channel --add https://nixos.org/channels/nixpkgs-unstable")
      node1.succeed("nix-channel --update")
      node1.succeed("nix-env -iA nixos.nvme-cli")
      print(node1.succeed("nvme discover -a 127.0.0.1 -s 8420 -t tcp -q nqn.2014-08.org.nvmexpress.discovery"))
      """

      #cmd = "${ mayastor-develop }/bin/mayastor-client nexus create 19b98ac8-c1ea-11ea-8e3b-d74f5d324a22 1GB nvmf://127.0.0.1:8420/nqn.2019-05.io.openebs:5b5b04ea-c1e3-11ea-bd82-a7d5cb04b391"
      #print(node1.succeed(cmd))
    '';
})

# Jenkins

Jenkins is used to implement CI/CD pipeline for Mayastor. We make use of Jenkins
pipeline plugin that allows us to maintain CI/CD as a code that is part of
Mayastor repository (Jenkinsfile). The goal of this document is to describe
steps needed to set up the Jenkins including the test executors (slaves in
Jenkins terminology). Details differ depending which environment the Jenkins is
set up in: AWS EC2 cloud, server with KVM guests, etc. We describe the most
important common steps for all environments. We make use of NixOS capabilities
for system configuration of nodes (as opposed to using ansible, salt, etc.).

## Master

1. /etc/nixos/configuration.nix file on the master:

   ```nix
   { pkgs, ... }:

   {
   imports = [ ./hardware.nix ];

   services.openssh.enable = true;
   services.jenkins.enable = true;

   networking.firewall.enable = false;
   networking.hostName = "ci-master";

   # Skip this block if the system comes with preconfigured network (in aws).
   # Replace the values below with something valid for your env.
   networking.interfaces.ens3.ipv4.addresses = [{
     address = "192.168.122.4";
     prefixLength = 24;
   }];
   networking.defaultGateway = "192.168.122.1";
   networking.nameservers = [ "192.168.122.1" ];
   networking.useDHCP = false;
   time.timeZone = "Europe/Prague";

   security.sudo.wheelNeedsPassword = false;

   # This user may or may not be useful. Managing the node using root
   # account is also possible.
   users.users.mayastor = {
     isNormalUser = true;
     extraGroups = [ "wheel" ]; # Enable ‘sudo’ for the user.
     openssh.authorizedKeys.keys = [ "ssh-rsa yourkey ..." ];
   };

   # Configuration of HTTPS reverse proxy (Jenkins does not speak https)
   security.acme.acceptTerms = true;
   security.acme.email = "jan.kryl@mayadata.io";
   services.nginx = {
     enable = true;
     recommendedProxySettings = true;
     recommendedTlsSettings = true;

     virtualHosts."mayastor-ci.mayadata.io" =  {
       enableACME = true
       forceSSL = true;
       sslCertificate = "/tmp/server.crt";
       sslCertificateKey = "/tmp/server.key";
       locations."/" = {
         root = "/var/lib/acme/.challenges";
         proxyPass = "http://127.0.0.1:8080";
         proxyWebsockets = true; # needed if you need to use WebSocket
         # required when the server wants to use HTTP Authentication
         extraConfig = "proxy_pass_header Authorization;";
       };
     };
   };

   environment.systemPackages = with pkgs; [
     wget curl vim git
   ];
   }
   ```

2. /etc/nixos/hardware.nix is not needed for AWS (the hardware part is
   preconfigured in nixos AMI):

   ```nix
   {
     imports = [
       <nixpkgs/nixos/modules/profiles/qemu-guest.nix>
     ];

     # Virtual disk will suit these settings:
     config = {
       fileSystems."/" = {
         device = "/dev/disk/by-label/nixos";
         fsType = "ext4";
         autoResize = true;
       };

       boot.growPartition = true;
       boot.kernelParams = [ "console=ttyS0" ];
       boot.loader.grub.device = "/dev/vda";
       boot.loader.timeout = 0;
     };
   }
   ```

3. Everytime a change is made to the nix files, `nix-rebuild switch` must be run.

4. Temporarily change configuration.nix to disable nginx proxy in virtual
   host so that "let's encrypt" SSL certificate can be generated. Enable proxy
   again after that.

5. Load initial Jenkins page. Create mayastor user and set a password.
   Don't install any plugins.

6. After initial configuration install following plugins:
   * blue ocean
   * ssh agent
   * multibranch scan webhook trigger
   * embeddable build status
   * pipeline stage view
   * slack
   * disable GitHub Multibranch Status

7. Enable read-only access for unauthenticated clients.

8. Join a slave to the master. Steps how to set up the slave are in the next
   section.

9. Create the first pipeline on blue ocean web page by selecting mayastor github
   repo.

10. github webhooks need to be configured too (out of scope).


## Slave

1. /etc/nixos/configuration.nix file on the slave:

   ```nix
   { pkgs, ... }:

   {
   imports = [ ./hardware.nix ./iscsid.nix ];

   boot.kernelPackages = pkgs.linuxPackages_5_7;

   services.openssh.enable = true;
   services.jenkinsSlave.enable = true;
   services.iscsid.enable = true;

   boot.kernelParams = ["hugepages=4096" "hugepagesz=2MB"];
   boot.initrd.kernelModules = ["xfs"];
   boot.kernelModules = [ "nbd" "xfs" "nvme_tcp" "kvm_intel" ];
   boot.extraModprobeConfig = "options kvm_intel nested=1";

   nix.gc = {
     automatic = true;
     dates = "daily";
   };
   nix.extraOptions = ''
     min-free = ${toString (10 * 1024 * 1024 * 1024)}
   '';

   virtualisation.docker.enable = true;

   networking.firewall.enable = false;
   networking.hostName = "ci-slave";

   # Skip this block if the system comes with preconfigured network (in aws).
   # Replace the values below by something valid for your env.
   networking.interfaces.ens3.ipv4.addresses = [{
     address = "192.168.122.6";
     prefixLength = 24;
   }];
   networking.defaultGateway = "192.168.122.1";
   networking.nameservers = [ "192.168.122.1" ];
   networking.useDHCP = false;
   time.timeZone = "Europe/Prague";

   security.sudo.wheelNeedsPassword = false;

   # This user may or may not be useful. Managing the node using root
   # account is also possible.
   users.users.mayastor = {
     isNormalUser = true;
     extraGroups = [ "wheel" "docker" ]; # Enable ‘sudo’ for the user.
     password = "";
     openssh.authorizedKeys.keys = [ "ssh-rsa your-key ..." ];
   };

   users.users.jenkins.extraGroups = [ "wheel" "docker" ];
   users.users.jenkins.openssh.authorizedKeys.keys = [ "ssh-rsa key used by Jenkins master ..." ];

   environment.systemPackages = with pkgs; [
     wget curl vim git jdk openiscsi nvme-cli lsof kubectl
   ];
   }
   ```

2. /etc/nixos/iscsid.nix file:

   ```nix
   { config, stdenv, pkgs, lib, ... }:
   with lib;
   {
     options = {
       services.iscsid = {
         enable = mkOption {
           type = types.bool;
           default = false;
           description = "enable iscsid running on startup";
         };
       };
     };
     config = mkIf config.services.iscsid.enable {
       systemd.services.iscsid = {
         description = "iscsid daemon";
         wantedBy = [ "basic.target" ];
         serviceConfig = {
           ExecStart =
             "${pkgs.openiscsi}/bin/iscsid -f -c ${pkgs.openiscsi}/etc/iscsi/iscsid.conf -i ${pkgs.openiscsi}/etc/iscsi/initiatorname.iscsi";
           KillMode = "process";
           Restart = "on-success";
         };
       };
     };
   }
   ```

3. Hardware file is the same as for the master (if needed).

4. Create /etc/docker/daemon.json, replace private registry IP in there and restart the docker daemon:
   ```
   {
     "insecure-registries" : ["192.168.1.60:5000"]
   }
   ```
   This will allow the worker node to push docker images to http registry.

5. You can repeat the steps and set as many slaves you want.


## qcow2 images

If you are building CI/CD using KVM, you will likely find following steps on
how to create qcow2 images useful.

1. Create default.nix file in your work directory (change the name and
   diskSize parameters to suite your needs):

   ```nix
   { pkgs ? import <nixpkgs> { }
   , system ? builtins.currentSystem
   }:
   let
     lib = pkgs.lib;
     nixos =  <nixpkgs/nixos>;
     configuration = import ./configuration.nix { inherit pkgs; };
     # Image config
     config = (import nixos { inherit system configuration; });
     make-disk-image = import <nixpkgs/nixos/lib/make-disk-image.nix>;
   in
   rec {
     runner = config.vm;
     image = make-disk-image {
       inherit pkgs lib;
       config = config.config;

       name = "ci-master";
       format = "qcow2";
       diskSize = 50 * 1024; # megabytes
     };
   }
   ```

2. Create configuration.nix file (and hardware.nix) depending if it's for
   master or slave.

3. Run `nix-build -A image`.

4. Copy created qcow2 image in result directory to work directory.

5. Create KVM domain (nixos.qcow2 is the image built in previous step).
   Following parameters are appropriate for the master node. Slave node
   will need more RAM and more vcpus.

   ```bash
   virt-install -n ci-master --import --cpu=host --os-type=generic --memory=2048 --vcpus=1 --disk /home/loo/qcow2/nixos.qcow2,format=qcow2,bus=virtio --network=bridge=virbr0,model=virtio
   ```

# TODO

* Seek ways how to automate setup of CI/CD pipeline even further using terraform, nix and scripts.
* The same as above specifically for Jenkins slaves.
* Explore (and potentially write our own) Jenkins plugin for dynamic spawning of the slaves in the cloud.

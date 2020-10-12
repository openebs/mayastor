# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.

{ stdenv
, busybox
, dockerTools
, e2fsprogs
, git
, lib
, moac
, writeScriptBin
, xfsprogs
, mayastor
, mayastor-dev
, mayastor-adhoc
, utillinux
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  env = stdenv.lib.makeBinPath [ busybox xfsprogs e2fsprogs utillinux ];

  # common props for all mayastor images
  mayastorImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
  mayastorCsiImageProps = {
    tag = version;
    created = "now";
    config = {
      Entrypoint = [ "/bin/mayastor-csi" ];
      Env = [ "PATH=${env}" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
  servicesImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
in
rec {
  mayastor-image = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor";
    contents = [ busybox mayastor ];
  });

  mayastor-dev-image = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor-dev";
    contents = [ busybox mayastor-dev ];
  });

  mayastor-adhoc-image = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor-adhoc";
    contents = [ busybox mayastor-adhoc ];
  });

  mayastorIscsiadm = writeScriptBin "mayastor-iscsiadm" ''
    #!${stdenv.shell}
    chroot /host /usr/bin/env -i PATH="/sbin:/bin:/usr/bin" iscsiadm "$@"
  '';

  mayastor-csi-image = dockerTools.buildLayeredImage (mayastorCsiImageProps // {
    name = "mayadata/mayastor-csi";
    contents = [ busybox mayastor mayastorIscsiadm ];
  });

  mayastor-csi-dev-image = dockerTools.buildImage (mayastorCsiImageProps // {
    name = "mayadata/mayastor-csi-dev";
    contents = [ busybox mayastor-dev mayastorIscsiadm ];
  });

  # The algorithm for placing packages into the layers is not optimal.
  # There are a couple of layers with negligable size and then there is one
  # big layer with everything else. That defeats the purpose of layering.
  moac-image = dockerTools.buildLayeredImage {
    name = "mayadata/moac";
    tag = version;
    created = "now";
    contents = [ busybox moac ];
    config = {
      Entrypoint = [ "${moac.out}/bin/moac" ];
      ExposedPorts = { "3000/tcp" = { }; };
      Env = [ "PATH=${moac.env}:${moac.out}/bin" ];
      WorkDir = "${moac.out}";
    };
    extraCommands = ''
      chmod u+w bin
      ln -s ${moac.out}/bin/moac bin/moac
      chmod u-w bin
    '';
  };

  services-kiiss-image = dockerTools.buildLayeredImage (servicesImageProps // {
    name = "mayadata/services-kiiss";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/kiiss" ]; };
  });

  services-kiiss-dev-image = dockerTools.buildImage (servicesImageProps // {
    name = "mayadata/services-kiiss-dev";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/kiiss" ]; };
  });
}

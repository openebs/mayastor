# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.
#
# We limit max number of image layers to 42 because there is a bug in
# containerd triggered when there are too many layers:
# https://github.com/containerd/containerd/issues/4684

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
  clientImageProps = {
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
  operatorImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
  };
  serviceImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
  };
  mayastorIscsiadm = writeScriptBin "mayastor-iscsiadm" ''
    #!${stdenv.shell}
    chroot /host /usr/bin/env -i PATH="/sbin:/bin:/usr/bin" iscsiadm "$@"
  '';
in
{
  mayastor = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor";
    contents = [ busybox mayastor ];
  });

  mayastor-dev = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor-dev";
    contents = [ busybox mayastor-dev ];
  });

  mayastor-adhoc = dockerTools.buildImage (mayastorImageProps // {
    name = "mayadata/mayastor-adhoc";
    contents = [ busybox mayastor-adhoc ];
  });

  mayastor-csi = dockerTools.buildLayeredImage (mayastorCsiImageProps // {
    name = "mayadata/mayastor-csi";
    contents = [ busybox mayastor mayastorIscsiadm ];
    maxLayers = 42;
  });

  mayastor-csi-dev = dockerTools.buildImage (mayastorCsiImageProps // {
    name = "mayadata/mayastor-csi-dev";
    contents = [ busybox mayastor-dev mayastorIscsiadm ];
  });

  # The algorithm for placing packages into the layers is not optimal.
  # There are a couple of layers with negligable size and then there is one
  # big layer with everything else. That defeats the purpose of layering.
  moac = dockerTools.buildLayeredImage {
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
      # workaround for detect-libc npm module unable to detect glibc system
      chmod u+w .
      mkdir -p usr/sbin
      touch usr/sbin/detect-glibc-in-nix-container
      chmod u-w .
    '';
    maxLayers = 42;
  };

  mayastor-client = dockerTools.buildImage (clientImageProps // {
    name = "mayadata/mayastor-client";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/mayastor-client" ]; };
  });

  # Release image of kiiss service.
  kiiss-service = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/kiiss-service";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/kiiss" ]; };
    maxLayers = 42;
  });

  # Development image of kiiss service.
  kiiss-service-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/kiiss-service-dev";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/kiiss" ]; };
  });

  # Release image of node service.
  node-service = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/node-service";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/node" ]; };
    maxLayers = 42;
  });

  # Development image of node service.
  node-service-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/node-service-dev";
    contents = [ busybox mayastor-dev ];
    config = { Entrypoint = [ "/bin/node" ]; };
  });

  # Release image of volume service.
  volume-service = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/volume-service";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/volume" ]; };
    maxLayers = 42;
  });

  # Development image of volume service.
  volume-service-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/volume-service-dev";
    contents = [ busybox mayastor-dev ];
    config = { Entrypoint = [ "/bin/volume" ]; };
  });

  # Release image of pool service.
  pool-service = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/pool-service";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/pool" ]; };
    maxLayers = 42;
  });

  # Development image of pool service.
  pool-service-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/pool-service-dev";
    contents = [ busybox mayastor-dev ];
    config = { Entrypoint = [ "/bin/pool" ]; };
  });

  # Release image of rest service.
  rest-service = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/rest-service";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/rest" ]; };
    maxLayers = 42;
  });

  # Development image of rest service.
  rest-service-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/rest-service-dev";
    contents = [ busybox mayastor-dev ];
    config = { Entrypoint = [ "/bin/rest" ]; };
  });

  # Release image of node operator.
  node-operator = dockerTools.buildLayeredImage (operatorImageProps // {
    name = "mayadata/node-operator";
    contents = [ busybox mayastor ];
    config = { Entrypoint = [ "/bin/node-op" ]; };
    maxLayers = 42;
  });

  # Development image of node operator.
  node-operator-dev = dockerTools.buildImage (operatorImageProps // {
    name = "mayadata/node-operator-dev";
    contents = [ busybox mayastor-dev ];
    config = { Entrypoint = [ "/bin/node-op" ]; };
  });
}

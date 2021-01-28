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
, xfsprogs
, utillinux
}:
let
  versionDrv = import ../../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  env = stdenv.lib.makeBinPath [ busybox xfsprogs e2fsprogs utillinux ];

  # common props for all service images
  serviceImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
  };
in
{
  # Release image of kiiss service.
  kiiss-image = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/kiiss-service";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/kiiss" ]; };
    maxLayers = 42;
  });

  # Development image of kiiss service.
  kiiss-image-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/kiiss-service-dev";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/kiiss" ]; };
  });

  # Release image of node service.
  node-image = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/node-service";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/node" ]; };
    maxLayers = 42;
  });

  # Development image of node service.
  node-image-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/node-service-dev";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/node" ]; };
  });

  # Release image of volume service.
  volume-image = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/volume-service";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/volume" ]; };
    maxLayers = 42;
  });

  # Development image of volume service.
  volume-image-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/volume-service-dev";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/volume" ]; };
  });

  # Release image of pool service.
  pool-image = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/pool-service";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/pool" ]; };
    maxLayers = 42;
  });

  # Development image of pool service.
  pool-image-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/pool-service-dev";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/pool" ]; };
  });

  # Release image of rest service.
  rest-image = dockerTools.buildLayeredImage (serviceImageProps // {
    name = "mayadata/rest-service";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/rest" ]; };
    maxLayers = 42;
  });

  # Development image of rest service.
  rest-image-dev = dockerTools.buildImage (serviceImageProps // {
    name = "mayadata/rest-service-dev";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/rest" ]; };
  });
}

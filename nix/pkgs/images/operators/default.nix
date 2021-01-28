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

  # common props for all operator images
  operatorImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
  };
in
{
  # Release image of node operator.
  node-image = dockerTools.buildLayeredImage (operatorImageProps // {
    name = "mayadata/node-operator";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/node-op" ]; };
    maxLayers = 42;
  });

  # Development image of node operator.
  node-image-dev = dockerTools.buildImage (operatorImageProps // {
    name = "mayadata/node-operator-dev";
    contents = [ busybox ];
    config = { Entrypoint = [ "/bin/node-op" ]; };
  });

}

# It would be cool to produce OCI images instead of docker images to
# avoid dependency on docker tool chain. Though the maturity of OCI
# builder in nixpkgs is questionable which is why we postpone this step.
#
# We limit max number of image layers to 42 because there is a bug in
# containerd triggered when there are too many layers:
# https://github.com/containerd/containerd/issues/4684

{ busybox
, dockerTools
, e2fsprogs
, git
, lib
, io-engine
, io-engine-dev
, stdenv
, utillinux
, writeScriptBin
, xfsprogs
}:
let
  versionDrv = import ../../lib/version.nix { inherit lib stdenv git; };
  version = builtins.readFile "${versionDrv}";
  path = lib.makeBinPath [ "/" busybox xfsprogs e2fsprogs utillinux ];

  # common props for all io-engine images
  ioEngineImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [
        "PATH=${path}"
        "RUST_BACKTRACE=1"
      ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/io-engine" ];
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
      Env = [ "PATH=${path}" ];
    };
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };

  mctl = writeScriptBin "mctl" ''
    /bin/io-engine-client "$@"
  '';
in
{
  mayastor-io-engine = dockerTools.buildImage (ioEngineImageProps // {
    name = "mayadata/mayastor-io-engine";
    contents = [ busybox io-engine mctl ];
  });

  mayastor-io-engine-dev = dockerTools.buildImage (ioEngineImageProps // {
    name = "mayadata/mayastor-io-engine-dev";
    contents = [ busybox io-engine-dev ];
  });

  mayastor-io-engine-client = dockerTools.buildImage (ioEngineImageProps // {
    name = "mayadata/mayastor-io-engine-client";
    contents = [ busybox io-engine ];
    config = { Entrypoint = [ "/bin/io-engine-client" ]; };
  });
}

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
, control-plane
, tini
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
  agentImageProps = {
    tag = version;
    created = "now";
    config = {
      Env = [ "PATH=${env}" ];
    };
  };
  build-control-plane-image = { build, name, binary, config ? { } }: dockerTools.buildImage {
    tag = version;
    created = "now";
    name = "mayadata/mayastor-${name}";
    contents = [ tini busybox control-plane.${build}.${name} ];
    config = { Entrypoint = [ "tini" "--" "${binary}" ]; } // config;
  };
  build-agent-image = { build, name, config ? { } }: build-control-plane-image {
    inherit build name;
    binary = "${name}-agent";
  };
  agent-images = { build }: {
    kiiss = build-agent-image { inherit build; name = "kiiss"; };
    node = build-agent-image { inherit build; name = "node"; };
    pool = build-agent-image { inherit build; name = "pool"; };
    volume = build-agent-image { inherit build; name = "volume"; };
    rest = build-agent-image {
      inherit build; name = "rest";
      config = { ExposedPorts = { "8080/tcp" = { }; "8081/tcp" = { }; }; };
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

  agents = agent-images { build = "release"; };
  agents-dev = agent-images { build = "debug"; };
}

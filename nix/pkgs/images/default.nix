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
, pkgs
, libspdk-fio
, stdenv
, utillinux
, writeScriptBin
, xfsprogs
, btrfs-progs
, runCommand
, tini
, img_tag ? ""
}:
let
  version = if img_tag != "" then img_tag else io-engine.version;
  path = lib.makeBinPath [ "/" busybox utillinux ];

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
    config = { };
    contents = [ busybox ];
    extraCommands = ''
      mkdir tmp
      mkdir -p var/tmp
    '';
  };
  spdk_fio_engine = runCommand "spdk_fio_engine" { } ''
    mkdir -p $out/lib
    cp ${pkgs.libspdk-fio.fio}/spdk_nvme $out/lib
    except=$(${pkgs.glibc.bin}/bin/ldd $out/lib/spdk_nvme | awk '{ print $3 }' | grep .so | xargs -I% echo "-e %" | tr '\n' ' ')
    # remove all references except the ones in spdk_nvme
    ${pkgs.nukeReferences}/bin/nuke-refs $except $out/lib/spdk_nvme
  '';

  fio = runCommand "fio_bin_only" { } ''
    mkdir -p $out/bin
    cp ${pkgs.fio}/bin/fio $out/bin/fio
  '';
  fio_wrapper = pkgs.writeShellScriptBin "fio" ''
    LD_PRELOAD=${spdk_fio_engine}/lib/spdk_nvme ${fio}/bin/fio "$@"
  '';

  mctl = writeScriptBin "mctl" ''
    /bin/io-engine-client "$@"
  '';
in
{
  mayastor-io-engine = dockerTools.buildImage (ioEngineImageProps // {
    name = "openebs/mayastor-io-engine";
    contents = [ busybox io-engine mctl ];
  });

  mayastor-io-engine-dev = dockerTools.buildImage (ioEngineImageProps // {
    name = "openebs/mayastor-io-engine-dev";
    contents = [ busybox io-engine-dev ];
  });

  mayastor-io-engine-client = dockerTools.buildImage (ioEngineImageProps // {
    name = "openebs/mayastor-io-engine-client";
    contents = [ busybox io-engine ];
    config = { Entrypoint = [ "/bin/io-engine-client" ]; };
  });

  mayastor-fio-spdk = dockerTools.buildImage (clientImageProps // {
    name = "openebs/mayastor-fio-spdk";
    contents = clientImageProps.contents ++ [ tini fio_wrapper ];
  });
}

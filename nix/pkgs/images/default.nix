{ stdenv
, busybox
, dockerTools
, e2fsprogs
, lib
, libaio
, libiscsi
, libspdk
, libudev
, liburing
, openssl
, utillinux
, writeScriptBin
, xfsprogs
, mayastor
, sources
}:

rec {

  env = stdenv.lib.makeBinPath [ busybox xfsprogs e2fsprogs ];

  # image that does not do a build
  mayastor-adhoc = stdenv.mkDerivation {
    name = "mayastor-dev";
    version = "1.0";
    src = [
      ../../../target/debug/mayastor
      ../../../target/debug/mayastor-agent
      ../../../target/debug/mayastor-client
      ../../../target/debug/mctl
    ];

    buildInputs = [
      libaio
      libiscsi.lib
      libspdk
      liburing
      libudev
      openssl
      xfsprogs
      e2fsprogs
    ];

    unpackPhase = ''
      for srcFile in $src; do
         cp $srcFile $(stripHash $srcFile)
      done
    '';
    dontBuild = true;
    dontConfigure = true;
    installPhase = ''
      mkdir -p $out/bin
      install * $out/bin
    '';
  };


  quick = dockerTools.buildImage {
    name = "mayadata/mayastor";
    tag = "adhoc";
    created = "now";
    contents = [ busybox mayastor-adhoc ];
    extraCommands = ''
      mkdir -p var/tmp
    '';
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastor-image-release = dockerTools.buildImage {
    name = "mayadata/mayastor";
    tag = sources.mayastor.branch;
    created = "now";
    contents = [ busybox mayastor ];
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastorIscsiadm = writeScriptBin "mayastor-iscsiadm" ''
    #!${stdenv.shell}
    chroot /host /usr/bin/env -i PATH="/sbin:/bin:/usr/bin" iscsiadm "$@"
  '';

  csi-release = dockerTools.buildLayeredImage {
    name = "mayadata/mayastor-grpc";
    tag = sources.mayastor.branch;
    created = "now";
    contents = [ busybox mayastor mayastorIscsiadm ];
    config = {
      Entrypoint = [ "/bin/mayastor-agent" ];
      Env = [ "PATH=${env}" ];
    };
  };


  # images during CI

  mayastor-develop = mayastor.override { release = false; };

  mayastor-image-develop = dockerTools.buildImage {
    name = "mayadata/mayastor";
    tag = "develop";
    created = "now";
    contents = [ busybox mayastor-develop ];
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor" ];
    };
  };

  mayastor-csi-develop = dockerTools.buildImage {
    name = "mayadata/mayastor-grpc";
    tag = "develop";
    created = "now";
    contents = [ busybox mayastor-develop ];
    config = {
      Env = [ "PATH=${env}" ];
      ExposedPorts = { "10124/tcp" = { }; };
      Entrypoint = [ "/bin/mayastor-agent" ];
    };
  };

}

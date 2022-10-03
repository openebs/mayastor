{ bashInteractive
, gnugrep
, dockerTools
, cachix
, cacert
, findutils
, coreutils
, git
, glibc
, gnutar
, gzip
, kmod
, lzma
, openssl
, nix
, stdenv
, xz
, utillinux
}:

dockerTools.buildImageWithNixDb {
  name = "ms-buildenv";
  tag = "now";
  contents = [
    ./root
    coreutils
    # add /bin/sh
    bashInteractive
    nix

    # runtime dependencies of nix
    cacert
    git
    gnutar
    gzip
    xz
    # needed for github actions
    glibc
    stdenv.cc.cc.lib

    # needed for losetup
    utillinux

    # needed for modprobe
    kmod
    findutils
    gnugrep

  ];

  extraCommands = ''
    # for /usr/bin/env
    mkdir usr
    ln -s ../bin usr/bin

    # make sure /tmp exists
    mkdir -m 1777 tmp

    # need a HOME
    mkdir -vp root
    mkdir -p var/tmp

  '';
  config = {
    Cmd = [ "/bin/bash" ];
    Env = [
      "ENV=/etc/profile.d/nix.sh"
      "BASH_ENV=/etc/profile.d/nix.sh"
      "NIX_BUILD_SHELL=/bin/bash"
      "NIX_PATH=nixpkgs=https://github.com/NixOS/nixpkgs/archive/3389f23412877913b9d22a58dfb241684653d7e9.tar.gz"
      "PAGER=cat"
      "PATH=/usr/bin:/bin"
      "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
      "USER=root"
      "LD_LIBRARY_PATH=/lib:/lib64"
    ];
  };
}


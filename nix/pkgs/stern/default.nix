{ stdenv, pkgs ? import <nixpkgs> {} }:
pkgs.stdenv.mkDerivation rec {
  name = "stern";
  version = "1.11.9";
  src = pkgs.fetchurl {
    url =
      "https://github.com/wercker/stern/releases/download/1.11.0/stern_linux_amd64";
    sha256 = "1nxpkqy8736jga6c3zdgdavm4cskvaw4z3j0nab7a31sdz19vcz0";
  };

  phases = [ "installPhase" ];
  installPhase = ''
    mkdir -p $out/bin
    cp $src/stern_linux_amd64 $out/bin/${name}
    chmod +x $out/bin/${name}
  '';

  meta = {
    description = "Multi pod and container log tailing for Kubernetes";
    license = stdenv.lib.licenses.apsl20;
    platforms = stdenv.lib.platforms.x86_64;
    maintainers = [ "jkryl" "gila" ];
  };
}

{ stdenv, pkgs ? import <nixpkgs> {} }:

stdenv.mkDerivation {
  name = "k9s";
  version = "0.9.2";
  src = pkgs.fetchurl {
    url =
      "https://github.com/derailed/k9s/releases/download/0.9.2/k9s_0.9.2_Linux_x86_64.tar.gz";
    sha256 = "08brnnmxi8zrsbmmn9sa0hpb129kxasrxg33iiclkny7m7hzi4kc";
  };

  phases = [ "installPhase" ];
  installPhase = ''
    mkdir -p $out/bin
    tar -xf $src -C $out/bin
    chmod +x $out/bin/$name
  '';

  meta = {
    description = "Kubernetes CLI To Manage Your Clusters In Style";
    license = stdenv.lib.licenses.apsl20;
    platforms = stdenv.lib.platforms.x86_64;
    maintainers = [ "jkryl" "gila" ];
  };
}

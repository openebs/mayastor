{}:
let
  rev = "10d60f7fae0afae261e5d556379f33777732fd27";
  sha256 = "0glk8ahm47r26w9pyh5vrhbgv091d2fah55yj4a8jliavbmfsv2s";
in
builtins.fetchTarball {
  url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
  inherit sha256;
}

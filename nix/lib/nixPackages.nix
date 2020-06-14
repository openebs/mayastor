{}:
let
  rev = "db31e48c5c8d99dcaf4e5883a96181f6ac4ad6f6";
  sha256 = "1j5j7vbnq2i5zyl8498xrf490jca488iw6hylna3lfwji6rlcaqr";

in
builtins.fetchTarball {
  url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
  inherit sha256;
}

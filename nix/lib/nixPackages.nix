{}:
let
  rev = "d8d19f249793966d1cce391d54203cc717764e63";
  sha256 = "0nb8d8bb4a7slml74i2c4aagcn0nz9pv447a7zxq2vp304r32qzj";

in
builtins.fetchTarball {
  url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
  inherit sha256;
}

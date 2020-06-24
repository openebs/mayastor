{ sources ? import ./sources.nix }: # import the sources
with
{
  overlay = _: pkgs:
    {
      niv = import sources.niv { }; # use the sources :)
    };
};
import sources.nixpkgs                  # and use them again!
{ overlays = [ overlay (import ./nix/mayastor-overlay.nix) ]; config = { }; }
  pkgs

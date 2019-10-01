## Overview

These are a collection of packages we need, or packages where we 
want to control the exact version(s) of.

The packages are imported through the `nix-shell` automatically. If you
run NixOS, read the following section.

## Adding adding the overlay(s)
```
$ mkdir -p ~/.config/nixpkgs/overlays
$ ln -s $(pwd)/nix/mayastor-overlay.nix ~/.config/nixpkgs/overlays/rust-overlay.nix
```

Now the package is integrated natively:

```
$ nix-env -qaP libiscsi
nixos.libiscsi  libiscsi-1.19.0
```

Like wise for rust nightly:

```
$ git clone https://github.com/mozilla/nixpkgs-mozilla.git
$ ln -s $(pwd)/nixpkgs-mozilla/rust-overlay.nix ~/.config/nixpkgs/overlays/rust-overlay.nix
```

With the overlay in place you can start nix-shell a within the project root.

## Configuration

To have iscsid enabled on your NixOS system you can use the service file
in your `configuration`.




---
name: reference_spdk_rs_custom_spdk
description: How to build mayastor/spdk-rs against a custom local SPDK checkout instead of the nix-pinned one
metadata:
  node_type: memory
  type: reference
  originSessionId: 6a81a615-94bf-4ff3-bd08-026c6e6faf7f
  modified: 2026-07-19T20:24:52.319Z
---

Source: https://github.com/openebs/spdk-rs#custom-spdk (develop README). Use when building [[project_mtcp_spdk_integration]]'s ffsock-enabled SPDK into mayastor.

**Build spdk-rs / mayastor against a custom local SPDK:**
1. Enter nix-shell WITHOUT the pinned SPDK, pointing at your checkout:
   `nix-shell --argstr spdk-path ~/myspdk`  (this sets `SPDK_ROOT_DIR` and implies `--argstr spdk none`). If you pass `--argstr spdk none` alone (no path), you must set `SPDK_ROOT_DIR` manually.
2. Get SPDK source (OpenEBS-patched branches), e.g.:
   `git clone git@github.com:openebs/spdk.git && cd spdk && git checkout -t origin/v24.01.x-mayastor && git submodule update --recursive --init`
   (User's actual tree is `/root/mayastore/spdk` = v25.05.x-mayastor @ 349e7c8c3.)
3. Build SPDK via the spdk-rs helper (inside the compatible nix-shell), NOT raw make/`make install DESTDIR` (unsupported for spdk-rs):
   `./build_scripts/build_spdk.sh configure`
   `./build_scripts/build_spdk.sh make`
   `./build_scripts/build_spdk.sh install <path>`
4. spdk-rs build.rs finds SPDK via `SPDK_ROOT_DIR`; with SPDK packages disabled in the nix-shell, editing SPDK + re-running `build_spdk.sh make` triggers Cargo to recompile spdk-rs (via build_logs dir change).

**How to apply:** to get ffsock into a running io-engine, add the module to the custom SPDK (done — see [[project_mtcp_spdk_integration]]), build it with build_spdk.sh, then build mayastor in `nix-shell --argstr spdk-path /root/mayastore/spdk`. NOTE our raw `make` in /root/mayastore/spdk verifies the SPDK build itself, but the mayastor-consumable build path is build_spdk.sh inside the nix-shell.

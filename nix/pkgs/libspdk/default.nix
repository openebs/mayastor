{ binutils
, cunit
, fetchFromGitHub
, pkg-config
, lcov
, lib
, libaio
, libbpf
, libbsd
, libelf
, libexecinfo
, libpcap
, liburing
, libuuid
, nasm
, cmake
, fio
, ninja
, jansson
, meson
, ncurses
, numactl
, openssl
, python3
, stdenv
, libtool
, yasm
, targetPlatform
, buildPlatform
, buildPackages
, llvmPackages
, pkgs
, gcc
, zlib
, autoconf
, automake
, with-fio
, multi-outputs ? false
}:
let
  fio-include = "${fio.dev}/include";
  fio-output = if multi-outputs then "fio" else "out";

  # Derivation attributes for production version of libspdk
  #
  # How to update to new libspdk commit:
  # 1. Push your SPDK commit to openebs repo.
  # 2. Copy git commit hash to 'rev' field, and copy first 9 digits of it
  #    to 'version' field.
  # 3. Leave old SHA256 intact.
  # 4. Login to your machine and run 'sudo nix store gc' to get rid of stale
  #    copies of SPDK package.
  # 5. Enter your mayastor 'nix-shell'.
  # 6. Wait until SPDK pkg build fails with
  #    "hash mismatch in fixed-output derivation" error.
  # 7. Copy SHA256 from 'got' of the error message to 'sha256' field.
  # 8. 'nix-shell' build must now succeed.
  drvAttrs = rec {
    version = "23.05-817317b";

    src = fetchFromGitHub {
      owner = "openebs";
      repo = "spdk";
      rev = "817317bb7e7681d90cc60a2e54a98e74f06fe8d4";
      sha256 = "sha256-cEul0oWNRr9LwCUTnlGXFEoIYSeUIv3KoXhI1cR8sh8=";
      fetchSubmodules = true;
    };

    nativeBuildInputs = [
      meson
      ninja
      pkg-config
      python3
      llvmPackages.clang
      gcc
      cmake
    ];

    buildInputs = [
      autoconf
      automake
      binutils
      jansson
      libaio
      libbpf
      libbsd
      libelf
      libexecinfo
      libpcap
      libtool
      liburing
      libuuid
      nasm
      ncurses
      numactl
      openssl
      (python3.withPackages (ps: with ps; [ pyelftools ]))
      yasm
      zlib
    ];

    configureFlags = (if (targetPlatform.config == "x86_64-unknown-linux-gnu") then
      [
        "--target-arch=nehalem"
        "--without-shared"
        "--without-crypto"
      ]
    else if (targetPlatform.config == "aarch64-unknown-linux-gnu") then
      [
        "--target-arch=armv8-a+crypto"
      ]
    else
      [ ]
    ) ++
    (if (targetPlatform.config != buildPlatform.config) then [ "--cross-prefix=${targetPlatform.config}" ] else [ ]) ++
    (if with-fio then [ "--with-fio=${fio-include}" ] else [ ]) ++
    [
      "--with-uring"
      "--without-uring-zns"
      "--disable-unit-tests"
      "--disable-tests"
    ];

    configurePhase = ''
      patchShebangs ./. > /dev/null
      export AS=yasm
      ./configure ${builtins.concatStringsSep " " configureFlags}
    '';
    enableParallelBuilding = true;

    hardeningDisable = [ "all" ];

    buildPhase = (if (targetPlatform.config == "aarch64-unknown-linux-gnu") then
      [
        "DPDKBUILD_FLAGS=-Dplatform=generic"
      ]
    else
      [ ]
    ) ++
    [
      "make -j`nproc`"
    ];

    installPhase = ''
      echo "installing SPDK to $out"
      mkdir -p $out/lib/pkgconfig
      mkdir $out/bin

      pushd include
      find . -type f -name "*.h" -exec install -vD "{}" $out/include/{} \;
      popd

      pushd lib
      find . -type f -name "*.h" -exec install -vD "{}" $out/include/spdk/lib/{} \;
      popd

      # copy private headers from bdev modules needed for creating of bdevs
      pushd module
      find . -type f -name "*.h" -exec install -vD "{}" $out/include/spdk/module/{} \;
      popd

      find . -executable -type f -name 'bdevperf' -exec install -vD "{}" $out/bin \;

      # copy libraries
      install -v build/lib/*.a                   $out/lib/
      install -v build/lib/pkgconfig/*.pc        $out/lib/pkgconfig/
      install -v dpdk/build/lib/*.a              $out/lib/
      install -v dpdk/build/lib/pkgconfig/*.pc   $out/lib/pkgconfig/
    '' + lib.optionalString targetPlatform.isx86_64 ''
      install -v isa-l/.libs/*.a                 $out/lib/
      install -v isa-l/*.pc                      $out/lib/pkgconfig/
      install -v isa-l-crypto/.libs/*.a          $out/lib/
      install -v isa-l-crypto/*.pc               $out/lib/pkgconfig/

      # fix paths in pkg config files
      build_dir=`pwd`
      for i in `ls $out/lib/pkgconfig/*.pc`;
      do
        echo "fixing pkg config paths in '$i' ..."
        sed -i "s,$build_dir/build/lib,$out/lib,g" $i
        sed -i "s,$build_dir/dpdk/build,$out,g" $i
        sed -i "s,$build_dir/intel-ipsec-mb/lib,$out/lib,g" $i
      done
    '' + lib.optionalString (with-fio && !multi-outputs) ''
      mkdir $out/fio
      cp build/fio/spdk_* $out/fio
    '' + lib.optionalString (with-fio && multi-outputs) ''
      mkdir $fio
      cp build/fio/spdk_* $fio
    '';

    outputs = [ "out" ] ++ lib.optional (fio-output != "out") fio-output;
  };
in
{
  release = llvmPackages.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk";
    dontStrip = false;
  });
  debug = llvmPackages.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk-dev";
    dontStrip = true;
    nativeBuildInputs = drvAttrs.nativeBuildInputs ++ [ cunit lcov ];
    buildInputs = drvAttrs.buildInputs ++ [ cunit lcov ];
    configurePhase = ''
      patchShebangs ./. > /dev/null
      export AS=yasm
      ./configure ${builtins.concatStringsSep " " (drvAttrs.configureFlags ++
      [
        "--enable-debug"
      ])}
    '';
    installPhase = drvAttrs.installPhase + ''
      echo "Copying test files"
      cp -ar test $out/test
    '';
  });
}

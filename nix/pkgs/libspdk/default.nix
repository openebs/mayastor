{ binutils
, cunit
, fetchFromGitHub
, pkg-config
, lcov
, lib
, libaio
, libbpf
, libelf
, liburing
, libuuid
, libpcap
, libbsd
, libexecinfo
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
, llvmPackages_11
, pkgs
, gcc
, zlib
}:
let
  # Derivation attributes for production version of libspdk
  drvAttrs = rec {
    version = "21.07-8489d57e8";

    src = fetchFromGitHub {
      owner = "openebs";
      repo = "spdk";
      rev = "8489d57e82e95c05c794f56a47f62bfd6c459b7b";
      sha256 = "LWYEBJ8JukR24ugWQ7qmM5O6LNZad38HWfcJROlUodU=";
      fetchSubmodules = true;
    };

    nativeBuildInputs = [
      meson
      ninja
      pkg-config
      python3
      llvmPackages_11.clang
      gcc
      cmake
    ];

    buildInputs = [
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
      zlib
    ];

    configureFlags = (if (targetPlatform.config == "x86_64-unknown-linux-gnu") then
      [
        "--target-arch=nehalem"
        "--with-crypto"
      ]
    else if (targetPlatform.config == "aarch64-unknown-linux-gnu") then
      [
        "--target-arch=armv8-a+crypto"
      ]
    else
      [ ]
    ) ++
    (if (targetPlatform.config != buildPlatform.config) then [ "--cross-prefix=${targetPlatform.config}" ] else [ ]) ++
    [
      "--without-isal"
      "--with-uring"
      "--disable-unit-tests"
      "--disable-tests"
    ];

    configurePhase = ''
      patchShebangs ./. > /dev/null
      ./configure ${builtins.concatStringsSep " " configureFlags}
    '';
    enableParallelBuilding = true;


    hardeningDisable = [ "all" ];

    buildPhase = ''
      make -j`nproc`
      find . -type f -name 'libspdk_event_nvmf.a' -delete
      find . -type f -name 'libspdk_sock_uring.a' -delete
      find . -type f -name 'libspdk_ut_mock.a' -delete
      find . -type f -name 'libspdk_bdev_blobfs.a' -delete
      find . -type f -name 'libspdk_bdev_ftl.a' -delete
      find . -type f -name 'libspdk_bdev_gpt.a' -delete
      find . -type f -name 'libspdk_bdev_passthru.a' -delete
      find . -type f -name 'libspdk_bdev_raid.a' -delete
      find . -type f -name 'libspdk_bdev_split.a' -delete
      find . -type f -name 'libspdk_bdev_zone_block.a' -delete

      $CC -shared -o libspdk.so \
        -lc -laio -lnuma -ldl -lrt -luuid -lpthread -lcrypto -luring \
        -Wl,--whole-archive \
        $(find build/lib -type f -name 'libspdk_*.a*' -o -name 'librte_*.a*') \
        $(find dpdk/build/lib -type f -name 'librte_*.a*') \
        $(find intel-ipsec-mb -type f -name 'libIPSec_*.a*') \
        -Wl,--no-whole-archive
    '';

    installPhase = ''
      mkdir -p $out/lib
      mkdir $out/bin

      pushd include
      find . -type f -name "*.h" -exec install -D "{}" $out/include/{} \;
      popd

      pushd lib
      find . -type f -name "*.h" -exec install -D "{}" $out/include/spdk/lib/{} \;
      popd

      # copy private headers from bdev modules needed for creating of bdevs
      pushd module
      find . -type f -name "*.h" -exec install -D "{}" $out/include/spdk/module/{} \;
      popd

      # copy over the library
      cp libspdk.so $out/lib

      echo $(find $out -type f -name '*.a*' -delete)
      find . -executable -type f -name 'bdevperf' -exec install -D "{}" $out/bin \;
    '';
  };
in
{
  release = llvmPackages_11.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk";
    separateDebugInfo = true;
    dontStrip = false;
  });
  debug = llvmPackages_11.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk-dev";
    separateDebugInfo = false;
    dontStrip = true;
    nativeBuildInputs = drvAttrs.nativeBuildInputs ++ [ cunit lcov ];
    buildInputs = drvAttrs.buildInputs ++ [ cunit lcov fio ];
    configurePhase = ''
      patchShebangs ./. > /dev/null
      ./configure ${builtins.concatStringsSep " " (drvAttrs.configureFlags ++
      [
        "--enable-debug"
        "--with-fio=${pkgs.fio}/include"
      ])}
    '';
    installPhase = drvAttrs.installPhase + ''
      echo "Copying test files"
      cp -ar test $out/test
      mkdir $out/fio
      cp build/fio/spdk_* $out/fio
    '';
  });
}

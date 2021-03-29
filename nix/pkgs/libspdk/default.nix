{ binutils
, cunit
, fetchFromGitHub
, pkg-config
, lcov
, libaio
, libiscsi
, libbpf
, libelf
, liburing
, libuuid
, libpcap
, libbsd
, libexecinfo
, nasm
, cmake
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
, gcc
, zlib
}:
let
  # Derivation attributes for production version of libspdk
  drvAttrs = rec {
    version = "21.01";

    src = fetchFromGitHub {
      owner = "openebs";
      repo = "spdk";
      rev = "3f85fb587d7a1013f3fab9304805dd943d95c0a2";
      sha256 = "0z7iw5xa2l4xrbl3zd4139mdyfd236gkswmysnkswmpmw7s6krsc";
      #sha256 = stdenv.lib.fakeSha256;
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
      libtool
      libaio
      libiscsi.dev
      liburing
      libuuid
      nasm
      ncurses
      numactl
      openssl
      libpcap
      libbsd
      jansson
      libbpf
      libelf
      libexecinfo
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
      "--with-iscsi-initiator"
      "--with-uring"
      "--disable-examples"
      "--disable-unit-tests"
      "--disable-tests"
    ];

    enableParallelBuilding = true;

    configurePhase = ''
      patchShebangs ./. > /dev/null
      ./configure ${builtins.concatStringsSep
          " "
          (builtins.filter
              (opt: (builtins.match "--build=.*" opt) == null)
              configureFlags)
      }
    '';

    hardeningDisable = [ "all" ];

    buildPhase = ''
      make -j`nproc`
      find . -type f -name 'libspdk_event_nvmf.a' -delete
      find . -type f -name 'libspdk_sock_uring.a' -delete
      find . -type f -name 'libspdk_ut_mock.a' -delete

      $CC -shared -o libspdk.so \
        -lc  -laio -liscsi -lnuma -ldl -lrt -luuid -lpthread -lcrypto \
        -luring \
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
    configureFlags = drvAttrs.configureFlags ++ [
      "--disable-tests"
      "--disable-unit-tests"
    ];
  });
  debug = llvmPackages_11.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk-dev";
    separateDebugInfo = false;
    dontStrip = true;
    buildInputs = drvAttrs.buildInputs ++ [ cunit lcov ];
    configureFlags = drvAttrs.configureFlags ++ [
      "--enable-debug"
    ];
    installPhase = drvAttrs.installPhase + ''
      echo "Copying test files"
      cp -ar test $out/test
    '';
  });
}

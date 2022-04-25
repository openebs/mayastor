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
, llvmPackages_11
, pkgs
, gcc
, zlib
}:
let
  # Derivation attributes for production version of libspdk
  drvAttrs = rec {
    version = "22.01-13f9d281c";

    src = fetchFromGitHub {
      owner = "openebs";
      repo = "spdk";
      rev = "13f9d281c93af370069fe17328f5971e95c19a2d";
      sha256 = "0avw6qsfqnsdzkk8ldbdvrfq0rrvi5wmjxa5p2cdq62mwdch3zz0";
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
    '';

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
      install -v intel-ipsec-mb/lib/*.a          $out/lib/

      # fix paths in pkg config files
      build_dir=`pwd`
      for i in `ls $out/lib/pkgconfig/*.pc`;
      do
        echo "fixing pkg config paths in '$i' ..."
        sed -i "s,$build_dir/build/lib,$out/lib,g" $i
        sed -i "s,$build_dir/dpdk/build,$out,g" $i
        sed -i "s,$build_dir/intel-ipsec-mb/lib,$out/lib,g" $i
      done
    '';
  };
in
{
  release = llvmPackages_11.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk";
    dontStrip = false;
  });
  debug = llvmPackages_11.stdenv.mkDerivation (drvAttrs // {
    pname = "libspdk-dev";
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

{ binutils
, callPackage
, enableDebug ? false
, fetchFromGitHub
, git
, libaio
, libiscsi
, libuuid
, nasm
, numactl
, openssl
, python
, rdma-core
, stdenv
}:

with stdenv.lib;

stdenv.mkDerivation rec {
  version = "20.01-mayastor";
  name = "libspdk";
  src = fetchFromGitHub {
    owner = "openebs";
    repo = "spdk";
    rev = "779b800ad7bf370a0f2ac1d6c30a2b15da04df6b";
    sha256 = "0774s5fvyds78b3bzp0w9acwci1m2zm9kz8b58ckdmy1ls0xsx43";
    fetchSubmodules = true;
  };

  buildInputs = [
    binutils
    libaio
    libiscsi.dev
    libuuid
    nasm
    numactl
    openssl
    python
    rdma-core
  ];

  CONFIGURE_OPTS = ''
    ${optionalString enableDebug "--enable-debug"}
    --without-isal --with-iscsi-initiator --with-rdma
    --with-internal-vhost-lib --disable-tests --with-dpdk-machine=native
    --with-crypto
  '';

  enableParallelBuilding = true;

  postPatch = ''
    patchShebangs ./.
    substituteInPlace dpdk/config/defconfig_x86_64-native-linux-gcc --replace native default

    # Do not build examples and app directories
    substituteInPlace Makefile --replace "examples app" ""

    # A workaround for https://bugs.dpdk.org/show_bug.cgi?id=356
    substituteInPlace dpdk/lib/Makefile --replace 'DEPDIRS-librte_vhost :=' 'DEPDIRS-librte_vhost := librte_hash'
  '';

  NIX_CFLAGS_COMPILE = "-mno-movbe -mno-lzcnt -mno-bmi -mno-bmi2 -march=corei7";
  hardeningDisable = [ "all" ];

  configurePhase = ''
    DESTDIR=$out ./configure $CONFIGURE_OPTS
  '';

  buildPhase = ''
    TARGET_ARCHITECTURE=corei7 make -j $NIX_BUILD_CORES

    # see README in spdk-sys why this needs to be done
    find . -type f -name 'libspdk_ut_mock.a' -delete
    find . -type f -name 'librte_vhost.a' -delete

    $CC -shared -o libspdk_fat.so \
    -lc -lrdmacm -laio -libverbs -liscsi -lnuma -ldl -lrt -luuid -lpthread -lcrypto \
    -Wl,--whole-archive \
    $(find build/lib -type f -name 'libspdk_*.a*' -o -name 'librte_*.a*') \
    $(find dpdk/build/lib -type f -name 'librte_*.a*') \
    $(find intel-ipsec-mb -type f -name 'libIPSec_*.a*') \
    -Wl,--no-whole-archive
  '';

  # todo -- split out in dev and normal pkg
  installPhase = ''
    # copy the public headers
    find include/ -type f -name "*.h" -exec install -D "{}" $out/{} \;

    # copy headers found in lib which are private
    pushd lib
    find . -type f -name "*.h" -exec install -D "{}" $out/include/{} \;
    popd

    # copy private headers from bdev modules needed for creating of bdevs
    pushd module
    find . -type f -name "*.h" -exec install -D "{}" $out/include/{} \;
    popd

    # copy over the library
    mkdir -p $out/lib
    cp libspdk_fat.so $out/lib
  '';

  dontStrip = enableDebug;
  separateDebugInfo = !enableDebug;
}

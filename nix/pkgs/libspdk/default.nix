{ stdenv, git, binutils, libaio, libuuid, numactl, openssl, python, rdma-core
, fetchFromGitHub, nasm, callPackage, libiscsi }:

stdenv.mkDerivation rec {
  version = "19.07.x-mayastor";
  name = "libspdk";
  src = fetchFromGitHub {
    rev = "1274d250a6f49731aecbbcc925fff208a25f4b95";
    repo = "spdk";
    owner = "openebs";
    sha256 = "148dp6nm8a2dglc8wk5yakjjd8r6s6drn1afff5afafa50fkcjgd";
    fetchSubmodules = true;
  };

  buildInputs =
    [ binutils libaio libiscsi libuuid nasm numactl openssl python rdma-core ];

  CONFIGURE_OPTS = ''
    --enable-debug --without-isal --with-iscsi-initiator --with-rdma   
        --with-internal-vhost-lib --disable-tests --with-dpdk-machine=native'';

  enableParallelBuilding = true;

  postPatch = ''
    patchShebangs ./.
    substituteInPlace dpdk/config/defconfig_x86_64-native-linux-gcc --replace native default
    substituteInPlace Makefile --replace examples ""
    substituteInPlace Makefile --replace app ""
  '';

  NIX_CFLAGS_COMPILE = "-mno-movbe -mno-lzcnt -mno-bmi -mno-bmi2 -march=corei7";
  hardeningDisable = [ "all" ];

  configurePhase = ''
    DESTDIR=$out ./configure $CONFIGURE_OPTS
  '';

  buildPhase = ''
    TARGET_ARCHITECTURE=corei7 make -j4

    # this should not be needed in the future as fixes are upstream that
    # are supposed to fix the shared lib building.

    find . -type f -name 'libspdk_ut_mock.a' -delete
    find . -type f -name 'librte_vhost.a' -delete

    $CC -shared -o libspdk_fat.so \
    -lc -lrdmacm -laio -libverbs -liscsi -lnuma -ldl -lrt -luuid -lpthread -lcrypto \
    -Wl,--whole-archive $(find build/lib -type f -name 'libspdk_*.a*' -o -name 'librte_*.a*') \
    -Wl,--whole-archive $(find dpdk/build/lib -type f -name 'librte_*.a*') \
    -Wl,--no-whole-archive
  '';

  # todo -- split out in dev and normal pkg
  installPhase = ''
    mkdir -p $out/lib
    cp -ar include $out
    cp libspdk_fat.so $out/lib
  '';

  dontStrip = true;
}


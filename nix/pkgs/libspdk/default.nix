{ stdenv, git, binutils, libaio, libuuid, numactl, openssl, python, rdma-core
, fetchFromGitHub, nasm, callPackage, libiscsi }:

stdenv.mkDerivation rec {
  version = "19.07.x-mayastor";
  name = "libspdk";
  src = fetchFromGitHub {
    owner = "openebs";
    repo = "spdk";
    rev = "0deb6044b7e4dd5073f7070378bb69942c53b78a";
    sha256 = "0qm5df0jrmqbhp5bzkmgpapzy3b6x69k2wnpf8f0bcbxh75pfx9s";
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
    --enable-debug --without-isal --with-iscsi-initiator --with-rdma
    --with-internal-vhost-lib --disable-tests --with-dpdk-machine=native
  '';

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
    find include/ -type f -name "*.h" -exec install -D "{}" $out/{} \;
    pushd lib
    find . -type f -name "*.h" -exec install -D "{}" $out/include/{} \;
    popd
    mkdir -p $out/lib
    cp libspdk_fat.so $out/lib
  '';

  dontStrip = true;
}


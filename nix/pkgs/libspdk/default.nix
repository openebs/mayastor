{ binutils
, callPackage
, cunit
, enableDebug ? false
, fetchFromGitHub
, pkgconfig
, git
, lcov
, libaio
, libiscsi
, liburing
, libuuid
, nasm
, ninja
, meson
, ncurses
, numactl
, openssl
, python3
, rdma-core
, stdenv
}:
with stdenv.lib;
stdenv.mkDerivation rec {

  pname = "libspdk";
  version = "20.07";

  src = fetchFromGitHub {
    owner = "openebs";
    repo = "spdk";
    rev = "7ea27936afa336b1e4292b6e4069231be8c5ba86";
    sha256 = "15ah963172jwbsfcp7y1kc2lbjy909ajjmj863a7vxfzh45xdy8v";
    #sha256 = stdenv.lib.fakeSha256;
    fetchSubmodules = true;
  };

  nativeBuildInputs = [
    meson
    ninja
    pkgconfig
    python3
  ];

  buildInputs = [
    binutils
    libaio
    libiscsi.dev
    liburing
    libuuid
    nasm
    ncurses
    numactl
    openssl
  ] ++ stdenv.lib.optionals enableDebug [ cunit lcov ];

  # add this once we merged this new option from upstream. The tests are going
  # to be split up in unit-test and test configure options. test will contain
  # utilities like bdevperf that is getting more features into it.

  #${enableFeature enableDebug "unit-tests"}

  configureFlags = [
    "${enableFeature enableDebug "tests"}"
    "${enableFeature enableDebug "unit-tests"}"
    "--target-arch=nehalem"
    "--without-isal"
    "--without-vhost"
    "--with-iscsi-initiator"
    "--with-crypto"
  ] ++ stdenv.lib.optionals (enableDebug) [ "--enable-debug" ];


  enableParallelBuilding = true;

  preConfigure = ''
    substituteInPlace dpdk/config/defconfig_x86_64-native-linux-gcc --replace native default
  '';

  configurePhase = ''
    patchShebangs ./.
    ./configure $configureFlags
  '';

  hardeningDisable = [ "all" ];

  buildPhase = ''
      make -j`nproc`
    find . -type f -name 'libspdk_ut_mock.a' -delete
    #find . -type f -name 'librte_vhost.a' -delete

    $CC -shared -o libspdk.so \
    -lc  -laio -liscsi -lnuma -ldl -lrt -luuid -lpthread -lcrypto \
    -Wl,--whole-archive \
    $(find build/lib -type f -name 'libspdk_*.a*' -o -name 'librte_*.a*') \
    $(find dpdk/build/lib -type f -name 'librte_*.a*') \
    $(find intel-ipsec-mb -type f -name 'libIPSec_*.a*') \
    -Wl,--no-whole-archive
  '';

 # postBuild = ''
 #   find . -type f -name 'libspdk_ut_mock.a' -delete
 #   #find . -type f -name 'librte_vhost.a' -delete

 #   $CC -shared -o libspdk.so \
 #   -lc  -laio -liscsi -lnuma -ldl -lrt -luuid -lpthread -lcrypto \
 #   -Wl,--whole-archive \
 #   $(find build/lib -type f -name 'libspdk_*.a*' -o -name 'librte_*.a*') \
 #   $(find dpdk/build/lib -type f -name 'librte_*.a*') \
 #   $(find intel-ipsec-mb -type f -name 'libIPSec_*.a*') \
 #   -Wl,--no-whole-archive

 # '';

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

    if [ $enableDebug ]
    then
      echo "copying files"
      cp -ar test $out/test
    fi

    echo $(find $out -type f -name '*.a*' -delete)
    find . -executable -type f -name 'bdevperf' -exec install -D "{}" $out/bin \;
  '';

  separateDebugInfo = !enableDebug;
  dontStrip = true;

}

{ stdenv
, fetchgit
, fetchpatch
, lib
, sources
}:

stdenv.mkDerivation rec {
  pname = "liburing";
  version = lib.removePrefix "liburing-" sources.liburing.branch;
  src = sources.liburing;

  separateDebugInfo = true;
  enableParallelBuilding = true;

  outputs = [ "out" "lib" "dev" "man" ];

  configurePhase = ''
    ./configure \
      --prefix=$out \
      --includedir=$dev/include \
      --libdir=$lib/lib \
      --libdevdir=$lib/lib \
      --mandir=$man/share/man \
  '';

  meta = with stdenv.lib; {
    description = "Userspace library for the Linux io_uring API";
    homepage = https://git.kernel.dk/cgit/liburing/;
    license = licenses.lgpl21;
    platforms = platforms.linux;
    maintainers = with maintainers; [ thoughtpolice ];
  };
}

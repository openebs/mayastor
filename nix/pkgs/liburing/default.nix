{ stdenv
, fetchgit
, fetchpatch
}:

stdenv.mkDerivation rec {
  pname = "liburing";
  version = "0.6";

  src = fetchgit {
    url = "http://git.kernel.dk/${pname}";
    rev = "f0c5c54945ae92a00cdbb43bdf3abaeab6bd3a23";
    sha256 = "06lrqx0ch8yszy6ck5y0kj8wn7i1bnjlrdgxbmr3g32ymic1hyln";
  };

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

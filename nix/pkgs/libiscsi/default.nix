{ stdenv, fetchgit, autoreconfHook }:

stdenv.mkDerivation rec {
  version = "1.19.0";
  name = "libiscsi-${version}";

  src = fetchgit {
    url = "https://github.com/sahlberg/libiscsi.git";
    rev = "eea5d3ba8e8fca98cde4b16b77f23a4fd683f3ba";
    sha256 = "0ajrkkg5awmi8m4b3mha7h07ylg18k252qprvk1sgq0qbyd66zy7";
  };

  outputs = [ "out" "bin" "lib" "dev" ];
  nativeBuildInputs = [ autoreconfHook ];
  meta = {
    description = "User space iscsi library";
    longDescription = ''
      Libiscsi is a client-side library to implement the iSCSI protocol that can be used
      to access the resources of an iSCSI target.

      The library is fully asynchronous with regards to iSCSI commands and SCSI tasks,
      but a synchronous layer is also provided for ease of use for simpler
      applications.
    '';
    homepage = "https://github.com/sahlberg/libiscsi";
    licenses = stdenv.lib.licenses.gpl2;
    maintainers = "gila@openebs.io";
    platforms = stdenv.lib.platforms.x86_64;
  };

}

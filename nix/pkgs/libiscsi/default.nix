{ stdenv, fetchgit, autoreconfHook, sources }:

stdenv.mkDerivation rec {
  version = sources.libiscsi.branch;
  name = "libiscsi-${version}";

  src = sources.libiscsi;

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

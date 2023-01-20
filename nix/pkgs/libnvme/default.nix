{ stdenv, lib, fetchgit, json_c, libuuid, meson, ninja, openssl, pkg-config, python3, sources }:

stdenv.mkDerivation rec {
  version = sources.libnvme.rev;
  name = "libnvme-${version}";

  src = sources.libnvme;

  nativeBuildInputs = [ json_c libuuid meson ninja openssl pkg-config python3 ];
  meta = {
    description = "Userspace NVMe library";
    longDescription = ''
      This is the libnvme development C library. libnvme provides type defintions for NVMe specification structures, enumerations, and bit fields, helper functions to construct, dispatch, and decode commands and payloads, and utilities to connect, scan, and manage nvme devices on a Linux system.
    '';
    homepage = "https://github.com/linux-nvme/libnvme";
    licenses = lib.licenses.lgpl21Plus;
    maintainers = [ "jonathan.teh@mayadata.io" ];
  };
}

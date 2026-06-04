{ stdenv, lib, fetchFromGitHub, pkgs }:

stdenv.mkDerivation rec {
  version = "v1.7";
  pname = "ublksrv";

  src = fetchFromGitHub {
    owner = "ublk-org";
    repo = "ublksrv";
    tag = version;
    sha256 = "sha256-g/qRWe3BReJ9RbEw/cFqpgUSMPzn/haS4WY4Hz9t+fw=";
  };

  nativeBuildInputs = with pkgs; [ pkg-config autoreconfHook ];
  buildInputs = with pkgs; [ liburing ];

  # fixup the version as the maintainer forgot to update the VERSION file..
  prePatch = ''
    echo ${version} > VERSION
  '';

  meta = {
    description = "The userspace part of the ublk framework";
    longDescription = ''
      This is the userspace daemon part(ublksrv) of the ublk framework, the other part is ublk driver which supports multiple queue.
    '';
    homepage = "https://github.com/ublk-org/ublksrv";
  };
}

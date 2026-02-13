{ stdenv, lib, fetchFromGitHub, pkgs }:

stdenv.mkDerivation rec {
  version = "v1.6";
  pname = "ublksrv";

  src = fetchFromGitHub {
    owner = "ublk-org";
    repo = "ublksrv";
    rev = version;
    sha256 = "sha256-bK+cf/qdlbyQ2gSObk8cqDkIWjaneNIpnlZDyywezf8=";
    leaveDotGit = true;
  };

  nativeBuildInputs = with pkgs; [ pkg-config autoreconfHook git ];
  buildInputs = with pkgs; [ liburing ];

  meta = {
    description = "The userspace part of the ublk framework";
    longDescription = ''
      This is the userspace daemon part(ublksrv) of the ublk framework, the other part is ublk driver which supports multiple queue.
    '';
    homepage = "https://github.com/ublk-org/ublksrv";
  };
}


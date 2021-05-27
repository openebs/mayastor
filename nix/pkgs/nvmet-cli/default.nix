{ lib, python3Packages, fetchurl }:

python3Packages.buildPythonApplication rec {
  pname = "nvmet-cli";
  version = "0.7";

  src = fetchurl {
    url = "ftp://ftp.infradead.org/pub/nvmetcli/nvmetcli-0.7.tar.gz";
    sha256 = "051y1b9w46azy35118154c353v3mhjkdzh6h59brdgn5054hayj2";
  };


  # in order to run the tests, nvmet module must be loaded
  # thats hard to do in our CI so we skip those by overriding
  # the checkphase below

  buildInputs = with python3Packages; [ nose2 ];

  propagatedBuildInputs = with python3Packages; [ configshell ];

  checkPhase = ''
    exit 0
  '';

  meta = with lib; {
    description = "NVMe target CLI";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}

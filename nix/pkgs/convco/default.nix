# Please delete me when https://github.com/NixOS/nixpkgs/pull/110375 is merged.
{ stdenv, lib, fetchFromGitHub, openssl, perl, pkg-config, rustPlatform
}:

rustPlatform.buildRustPackage rec {
  pname = "convco";
  version = "0.3.2";

  src = fetchFromGitHub {
    owner = "convco";
    repo = pname;
    rev = "v${version}";
    sha256 = "0fqq6irbq1aikhhw08gc9kp0vbk2aminfbvwdlm58cvywyq91bn4";
  };

  cargoSha256 = "073sfv42fbl8rjm3dih1ghs9vq75mjshp66zdzdan2dmmrnw5m9z";

  nativeBuildInputs = [ openssl perl pkg-config ];

  meta = with lib; {
    description = "A Conventional commit cli";
    homepage = "https://github.com/convco/convco";
    license = with licenses; [ mit ];
    maintainers = [
        {
            email = "operator+nix@hoverbear.org";
            github = "hoverbear";
            githubId = 130903;
            name = "Ana Hobden";
        }
    ];
  };
}

{ lib, stdenv, git }:
let
  whitelistSource = src: allowedPrefixes:
    builtins.filterSource
      (path: type:
        lib.any
          (allowedPrefix:
            lib.hasPrefix (toString (src + "/${allowedPrefix}")) path)
          allowedPrefixes)
      src;
in
stdenv.mkDerivation {
  name = "io-engine-version";
  src = whitelistSource ../../. [ ".git" ];

  # Newer git versions check directory ownership when executing commands.
  # Nix builds in directories with user/group different from the login user,
  # so git complains about that with the following error:
  #     fatal: detected dubious ownership in repository at $src
  #     To add an exception for this directory, call:
  #     git config --global --add safe.directory $src
  #
  # As git suggests, we marks $src as 'safe.directory' by overriding
  # the global git config with "-c safe.directory=$src" option.

  buildCommand = ''
    cd $src
    vers=$(${git}/bin/git -c safe.directory=$src describe --exact-match 2>/dev/null || echo "")
    if [ -z "$vers" ]; then
      vers=`${git}/bin/git -c safe.directory=$src rev-parse --short=12 HEAD`
    fi
    echo -n $vers >$out
  '';
}

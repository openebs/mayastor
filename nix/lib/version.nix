{ lib, stdenv, git, sourcer, tag ? "" }:
stdenv.mkDerivation {
  name = "io-engine-version";
  src = sourcer.git-src;
  outputs = [ "out" "long" "tag_or_long" ];

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
    vers=${tag}
    if [ -z "$vers" ]; then
      vers=$(${git}/bin/git -c safe.directory=$src describe --exact-match 2>/dev/null || echo "")
    fi
    if [ -z "$vers" ]; then
      vers=`${git}/bin/git -c safe.directory=$src rev-parse --short=12 HEAD`
    fi
    echo -n $vers >$out

    if [ "${tag}" != "" ]; then
      vers="${tag}-0-g$(${git}/bin/git -c safe.directory=$src rev-parse --short=12 HEAD)"
    else
      vers=$(${git}/bin/git -c safe.directory=$src describe --abbrev=12 --always --long)
    fi
    echo -n $vers >$long

    # when we point to a tag, it's just the tag
    vers=${tag}
    if [ -z "$vers" ]; then
        vers=$(${git}/bin/git -c safe.directory=$src describe --abbrev=12 --always)
    fi
    echo -n $vers >$tag_or_long
  '';
}

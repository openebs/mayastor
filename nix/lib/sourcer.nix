{ lib, stdenv, git, tag ? "" }:
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
{
  git-src = whitelistSource ../../. [ ".git" ];
  repo-org = whitelistSource ../../utils/dependencies/scripts [ "git-org-name.sh" ];
}

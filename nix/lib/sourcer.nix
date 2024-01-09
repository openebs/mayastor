{ lib, stdenv, git, tag ? "" }:
let
  whitelistSource = src: allowedPrefixes:
    builtins.path {
      filter = (path: type:
        lib.any
          (allowedPrefix:
            (lib.hasPrefix (toString (src + "/${allowedPrefix}")) path) ||
            (type == "directory" && lib.hasPrefix path (toString (src + "/${allowedPrefix}")))
          )
          allowedPrefixes);
      path = src;
      name = "io-engine";
    };
in
{
  git-src = whitelistSource ../../. [ ".git" ];
  repo-org = whitelistSource ../../utils/dependencies/scripts [ "git-org-name.sh" ];
}

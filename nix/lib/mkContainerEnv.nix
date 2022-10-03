#
# taken from docker nix repo to mush a user
# environment into a container to mimic as
# normal OS
#

{ writeText, lib }:
{ derivations }:
# Supporting code
with builtins;
let
  # back-compat
  isPath = builtins.isPath or (x: builtins.typeOf x == "path");

  # Escape Nix strings
  stringEscape = str:
    ''"'' + (replaceStrings [ "\\" ''"'' "\n" "\r" "  " ] [
      "\\\\"
      "\\"
      "\\n"
      "\\r"
      "\\t"
    ]
      str) + ''"'';

  # Like builtins.JSON but to output Nix code
  toNix = value:
    if isString value
    then
      stringEscape value
    else
      if isInt value
      then
        toString value
      else
        if isPath value
        then
          toString value
        else
          if true == value
          then
            "true"
          else
            if false == value
            then
              "false"
            else
              if null == value
              then
                "null"
              else
                if isAttrs value
                then
                  "{ " + concatStringsSep " "
                    (lib.mapAttrsToList (k: v: "${k} = ${toNix v};") value) + " }"
                else
                  if isList value
                  then
                    "[ ${concatStringsSep " " (map toNix value)} ]"
                  else
                    throw "type ${typeOf value} not supported";

  # Generate a nix-env compatible manifest.nix file
  genManifest = drv:
    let
      outputs = drv.meta.outputsToInstall or
        # install the first output
        [ (head drv.outputs) ];

      base = {
        inherit (drv) meta name outPath system type;
        out = { inherit (drv) outPath; };
        inherit outputs;
      };

      toOut = name: { outPath = drv.${name}.outPath; };

      outs = lib.genAttrs outputs toOut;
    in
    base // outs;

  writeManifest = derivations:
    writeText "env-manifest.nix" (toNix (map genManifest derivations));
in
import <nix/buildenv.nix> {
  inherit derivations;
  manifest = writeManifest derivations;
}

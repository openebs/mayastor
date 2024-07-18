{ rust ? "stable"
, spdk ? "develop"
, spdk-path ? null
} @ args:
import ./ci.nix {
  inherit rust;
  inherit spdk;
  inherit spdk-path;
}

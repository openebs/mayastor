{ rust ? "stable"         # "stable" "nightly" "asan" "none"
, spdk ? "develop"        # *"dev" "rel" "fio" "local" path
, spdk-path ? null
} @ args:
  import ./ci.nix args

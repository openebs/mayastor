let
  sources = import ../../nix/sources.nix;
  pkgs = import sources.nixpkgs {
    overlays = [
      (_: _: { inherit sources; })
      (import ../../nix/mayastor-overlay.nix)
    ];
  };
in
with pkgs;
mkShell {
  buildInputs = [
    (python3.withPackages (ps: with ps; [ grpcio grpcio-tools ]))
    python3Packages.virtualenv
  ];
  shellHook = ''
    virtualenv --no-setuptools venv
    source venv/bin/activate
    pip install -r requirements.txt
    python -m grpc_tools.protoc -I `realpath ../../rpc/proto` --python_out=. --grpc_python_out=.  mayastor.proto
  '';
}

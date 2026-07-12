{
  description = "Raft consensus algorithm implementation in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{
      flake-parts,
      rust-overlay,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      perSystem =
        { system, ... }:
        let
          pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [ (import rust-overlay) ];
          };
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
            ];
          };
          nightlyRustfmt = pkgs.rust-bin.nightly.latest.rustfmt;
        in
        {
          devShells.default = pkgs.mkShell {
            packages = [
              rustToolchain
              nightlyRustfmt
              pkgs.nixd
              pkgs.nixfmt-rfc-style
            ];

            shellHook = ''
              export RUSTFMT="${nightlyRustfmt}/bin/rustfmt"

              echo "Raft-rs development environment"
              echo "Rust version: $(rustc --version)"
            '';
          };
        };
    };
}

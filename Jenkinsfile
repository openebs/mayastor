pipeline {
  agent none

  stages {
    stage('linter') {
      agent { label 'nixos-mayastor' }
      steps {
        sh 'nix-shell --run "cargo fmt --all -- --check"'
        sh 'nix-shell --run "cargo clippy --all-targets -- -D warnings"'
        sh 'nix-shell --run "./scripts/js-check.sh"'
      }
    }
    stage('test') {
      parallel {
        stage('rust unit tests') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'nix-shell --run "./scripts/cargo-test.sh"'
          }
        }
        stage('mocha api tests') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'nix-shell --run "./scripts/node-test.sh"'
          }
        }
        stage('nix tests') {
          agent { label 'nixos-mayastor-kvm' }
          steps {
            sh 'nix-build ./nix/test -A rebuild'
            sh 'nix-build ./nix/test -A fio_nvme_basic'
            sh 'nix-build ./nix/test -A nvmf_distributed'
            sh 'nix-build ./nix/test -A nvmf_ports'
          }
        }
        stage('moac unit tests') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'nix-shell --run "./scripts/moac-test.sh"'
          }
        }
      }
    }
    stage('images') {
      agent { label 'nixos-mayastor' }
      steps {
        sh 'nix-build --no-out-link -A images.mayastor-dev-image'
        sh 'nix-build --no-out-link -A images.mayastor-csi-dev-image'
        sh 'nix-build --no-out-link -A images.moac-image'
      }
    }
  }
}
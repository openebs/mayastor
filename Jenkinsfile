pipeline {
  agent none
  triggers {
    cron('0 2 * * *')
  }

  stages {
    stage('linter') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        anyOf {
          allOf {
            branch 'staging'
            triggeredBy 'SCMTrigger'
          }
          allOf {
            branch 'trying'
            triggeredBy 'SCMTrigger'
          }
        }
      }
      steps {
        sh 'nix-shell --run "cargo fmt --all -- --check"'
        sh 'nix-shell --run "cargo clippy --all-targets -- -D warnings"'
        sh 'nix-shell --run "./scripts/js-check.sh"'
      }
    }
    stage('test') {
      when {
        beforeAgent true
        anyOf {
          allOf {
            branch 'staging'
            triggeredBy 'SCMTrigger'
          }
          allOf {
            branch 'trying'
            triggeredBy 'SCMTrigger'
          }
          allOf {
            branch 'develop'
            anyOf {
              triggeredBy 'TimerTrigger'
              triggeredBy cause: 'UserIdCause'
            }
          }
        }
      }
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
          post {
            always {
              junit '*-xunit-report.xml'
            }
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
          post {
            always {
              junit 'moac-xunit-report.xml'
            }
          }
        }
        stage('dev images') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'nix-build --no-out-link -A images.mayastor-dev-image'
            sh 'nix-build --no-out-link -A images.mayastor-csi-dev-image'
            sh 'nix-build --no-out-link -A images.moac-image'
            sh 'nix-store --delete /nix/store/*docker-image*'
          }
        }
      }
    }
    stage('push images') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        anyOf {
          allOf {
            branch 'master'
            triggeredBy 'SCMTrigger'
            anyOf {
              triggeredBy 'SCMTrigger'
              triggeredBy cause: 'UserIdCause'
            }
          }
          allOf {
            branch 'release/*'
            anyOf {
              triggeredBy 'SCMTrigger'
              triggeredBy cause: 'UserIdCause'
            }
          }
          allOf {
            branch 'develop'
            anyOf {
              triggeredBy 'TimerTrigger'
              triggeredBy cause: 'UserIdCause'
            }
          }
        }
      }
      steps {
        withCredentials([usernamePassword(credentialsId: 'dockerhub', usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
          sh 'echo $PASSWORD | docker login -u $USERNAME --password-stdin'
        }
        sh './scripts/release.sh'
      }
      post {
        always {
          sh 'docker logout'
          sh 'docker image prune --all --force'
        }
      }
    }
  }
}

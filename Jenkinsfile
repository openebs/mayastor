#!/usr/bin/env groovy

// On-demand E2E infra configuration
// https://mayadata.atlassian.net/wiki/spaces/MS/pages/247332965/Test+infrastructure#On-Demand-E2E-K8S-Clusters

def e2e_build_cluster_job='k8s-build-cluster' // Jenkins job to build cluster
def e2e_destroy_cluster_job='k8s-destroy-cluster' // Jenkins job to destroy cluster
// Environment to run e2e test in (job param of $e2e_build_cluster_job)
def e2e_environment="hcloud-kubeadm"
// Global variable to pass current k8s job between stages
def k8s_job=""

xray_projectkey='MQ'
xray_on_demand_testplan='MQ-1'
xray_nightly_testplan='MQ-17'
xray_test_execution_type='10059'

// Searches previous builds to find first non aborted one
def getLastNonAbortedBuild(build) {
  if (build == null) {
    return null;
  }

  if(build.result.toString().equals("ABORTED")) {
    return getLastNonAbortedBuild(build.getPreviousBuild());
  } else {
    return build;
  }
}

// Send out a slack message if branch got broken or has recovered
def notifySlackUponStateChange(build) {
  def cur = build.getResult()
  def prev = getLastNonAbortedBuild(build.getPreviousBuild())?.getResult()
  if (cur != prev) {
    if (cur == 'SUCCESS') {
      slackSend(
        channel: '#mayastor-backend',
        color: 'normal',
        message: "Branch ${env.BRANCH_NAME} has been fixed :beers: (<${env.BUILD_URL}|Open>)"
      )
    } else if (prev == 'SUCCESS') {
      slackSend(
        channel: '#mayastor-backend',
        color: 'danger',
        message: "Branch ${env.BRANCH_NAME} is broken :face_with_raised_eyebrow: (<${env.BUILD_URL}|Open>)"
      )
    }
  }
}

def getTestPlan() {
  def causes = currentBuild.getBuildCauses()
  for(cause in causes) {
    if ("${cause}".contains("hudson.triggers.TimerTrigger\$TimerTriggerCause")) {
      return xray_nightly_testplan
    }
  }
  return xray_on_demand_testplan
}

// Will ABORT current job for cases when we don't want to build
if (currentBuild.getBuildCauses('jenkins.branch.BranchIndexingCause') &&
    BRANCH_NAME == "develop") {
    print "INFO: Branch Indexing, aborting job."
    currentBuild.result = 'ABORTED'
    return
}

// Only schedule regular builds on develop branch, so we don't need to guard against it
String cron_schedule = BRANCH_NAME == "develop" ? "0 2 * * *" : ""
// Some long e2e tests are not suitable to be run for each PR
boolean run_extended_e2e_tests = (env.BRANCH_NAME != 'staging' && env.BRANCH_NAME != 'trying') ? true : false

pipeline {
  agent none
  options {
    timeout(time: 2, unit: 'HOURS')
  }
  triggers {
    cron(cron_schedule)
  }

  stages {
    stage('init') {
      agent { label 'nixos-mayastor' }
      steps {
        step([
          $class: 'GitHubSetCommitStatusBuilder',
          contextSource: [
            $class: 'ManuallyEnteredCommitContextSource',
            context: 'continuous-integration/jenkins/branch'
          ],
          statusMessage: [ content: 'Pipeline started' ]
        ])
      }
    }
    stage('linter') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        not {
          anyOf {
            branch 'master'
            branch 'release/*'
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
        not {
          anyOf {
            branch 'master'
            branch 'release/*'
          }
        }
      }
      parallel {
        stage('rust unit tests') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'printenv'
            sh 'nix-shell --run "./scripts/cargo-test.sh"'
          }
          post {
            always {
              // in case of abnormal termination of any nvmf test
              sh 'sudo nvme disconnect-all'
            }
          }
        }
        stage('grpc tests') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'printenv'
            sh 'nix-shell --run "./scripts/grpc-test.sh"'
          }
          post {
            always {
              junit '*-xunit-report.xml'
            }
          }
        }
        stage('moac unit tests') {
          agent { label 'nixos-mayastor' }
          steps {
            sh 'printenv'
            sh 'nix-shell --run "./scripts/moac-test.sh"'
          }
          post {
            always {
              junit 'moac-xunit-report.xml'
            }
          }
        }
        stage('e2e tests') {
          stages {
            stage('e2e docker images') {
              agent { label 'nixos-mayastor' }
              steps {
                // e2e tests are the most demanding step for space on the disk so we
                // test the free space here rather than repeating the same code in all
                // stages.
                sh "./scripts/reclaim-space.sh 10"
                // Build images (REGISTRY is set in jenkin's global configuration).
                // Note: We might want to build and test dev images that have more
                // assertions instead but that complicates e2e tests a bit.
                sh "./scripts/release.sh --alias-tag ci --registry \"${env.REGISTRY}\""
                // Always remove all docker images because they are usually used just once
                // and underlaying pkgs are already cached by nix so they can be easily
                // recreated.
              }
              post {
                always {
                  sh 'docker image prune --all --force'
                }
              }
            }
            stage('build e2e cluster') {
              agent { label 'nixos' }
              steps {
                script {
                  k8s_job=build(
                    job: "${e2e_build_cluster_job}",
                    propagate: true,
                    wait: true,
                    parameters: [[
                      $class: 'StringParameterValue',
                      name: "ENVIRONMENT",
                      value: "${e2e_environment}"
                    ]]
                  )
                }
              }
            }
            stage('run e2e') {
              agent { label 'nixos-mayastor' }
              environment {
                GIT_COMMIT_SHORT = sh(
                  // using printf to get rid of trailing newline
                  script: "printf \$(git rev-parse --short ${GIT_COMMIT})",
                  returnStdout: true
                )
                KUBECONFIG = "${env.WORKSPACE}/${e2e_environment}/modules/k8s/secrets/admin.conf"
              }
              steps {
                // FIXME(arne-rusek): move hcloud's config to top-level dir in TF scripts
                sh """
                  mkdir -p "${e2e_environment}/modules/k8s/secrets"
                """
                copyArtifacts(
                    projectName: "${k8s_job.getProjectName()}",
                    selector: specific("${k8s_job.getNumber()}"),
                    filter: "${e2e_environment}/modules/k8s/secrets/admin.conf",
                    target: "",
                    fingerprintArtifacts: true
                )
                sh 'kubectl get nodes -o wide'
                script {
                  def cmd = "./scripts/e2e-test.sh --device /dev/sdb --tag \"${env.GIT_COMMIT_SHORT}\" --registry \"${env.REGISTRY}\""
                  if (run_extended_e2e_tests) {
                    cmd = cmd + " --extended"
                  }
                  sh "nix-shell --run '${cmd}'"
                }
              }
              post {
                failure {
                  script {
                    withCredentials([string(credentialsId: 'HCLOUD_TOKEN', variable: 'HCLOUD_TOKEN')]) {
                      e2e_nodes=sh(
                        script: """
                          nix-shell -p hcloud --run 'hcloud server list' | grep -e '-${k8s_job.getNumber()} ' | awk '{ print \$2" "\$4 }'
                        """,
                        returnStdout: true
                      ).trim()
                    }
                    // Job name for multi-branch is Mayastor/<branch> however
                    // in URL jenkins requires /job/ in between for url to work
                    urlized_job_name=JOB_NAME.replaceAll("/", "/job/")
                    self_url="${JENKINS_URL}job/${urlized_job_name}/${BUILD_NUMBER}"
                    self_name="${JOB_NAME}#${BUILD_NUMBER}"
                    build_cluster_run_url="${JENKINS_URL}job/${k8s_job.getProjectName()}/${k8s_job.getNumber()}"
                    build_cluster_destroy_url="${JENKINS_URL}job/${e2e_destroy_cluster_job}/buildWithParameters?BUILD=${k8s_job.getProjectName()}%23${k8s_job.getNumber()}"
                    kubeconfig_url="${JENKINS_URL}job/${k8s_job.getProjectName()}/${k8s_job.getNumber()}/artifact/hcloud-kubeadm/modules/k8s/secrets/admin.conf"
                    slackSend(
                      channel: '#mayastor-backend',
                      color: 'danger',
                      message: "E2E k8s cluster <$build_cluster_run_url|#${k8s_job.getNumber()}> left running due to failure of " +
                        "<$self_url|$self_name>. Investigate using <$kubeconfig_url|kubeconfig>, or ssh as root to:\n" +
                        "```$e2e_nodes```\n" +
                        "And then <$build_cluster_destroy_url|destroy> the cluster.\n" +
                        "Note: you need to click `proceed` and will get an empty page when using destroy link. " +
                        "(<https://mayadata.atlassian.net/wiki/spaces/MS/pages/247332965/Test+infrastructure#On-Demand-E2E-K8S-Clusters|doc>)"
                    )
                  }
                }
                always { // always send the junit results back to Xray and Jenkins
                  junit 'e2e.*.xml'
                  script {
                    def xray_testplan = getTestPlan()
                    step([
                      $class: 'XrayImportBuilder',
                      endpointName: '/junit/multipart',
                      importFilePath: 'e2e.*.xml',
                      importToSameExecution: 'true',
                      projectKey: "${xray_projectkey}",
                      testPlanKey: "${xray_testplan}",
                      serverInstance: "${env.JIRASERVERUUID}",
                      inputInfoSwitcher: 'fileContent',
                      importInfo: """{
                        "fields": {
                          "summary": "Build ${env.BUILD_NUMBER}",
                          "project": {
                            "key": "${xray_projectkey}"
                          },
                          "issuetype": {
                            "id": "${xray_test_execution_type}"
                          },
                          "description": "Results for build ${env.BUILD_NUMBER} at ${env.BUILD_URL}"
                        }
                      }"""
                    ])
                  }
                }
              }
            }
            stage('destroy e2e cluster') {
              agent { label 'nixos' }
              steps {
                script {
                  build(
                    job: "${e2e_destroy_cluster_job}",
                    propagate: true,
                    wait: true,
                    parameters: [
                      [
                        $class: 'StringParameterValue',
                        name: "ENVIRONMENT",
                        value: "${e2e_environment}"
                      ],
                      [
                        $class: 'RunParameterValue',
                        name: "BUILD",
                        runId:"${k8s_job.getProjectName()}#${k8s_job.getNumber()}"
                      ]
                    ]
                  )
                }
              }
            }
          }
        }
      }
    }
    stage('push images') {
      agent { label 'nixos-mayastor' }
      when {
        beforeAgent true
        anyOf {
          branch 'master'
          branch 'release/*'
          branch 'develop'
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

  // The main motivation for post block is that if all stages were skipped
  // (which happens when running cron job and branch != develop) then we don't
  // want to set commit status in github (jenkins will implicitly set it to
  // success).
  post {
    always {
      node(null) {
        script {
          // If no tests were run then we should neither be updating commit
          // status in github nor send any slack messages
          if (currentBuild.result != null) {
            step([
              $class: 'GitHubCommitStatusSetter',
              errorHandlers: [[$class: "ChangingBuildStatusErrorHandler", result: "UNSTABLE"]],
              contextSource: [
                $class: 'ManuallyEnteredCommitContextSource',
                context: 'continuous-integration/jenkins/branch'
              ],
              statusResultSource: [
                $class: 'ConditionalStatusResultSource',
                results: [
                  [$class: 'AnyBuildResult', message: 'Pipeline result', state: currentBuild.getResult()]
                ]
              ]
            ])
            if (env.BRANCH_NAME == 'develop') {
              notifySlackUponStateChange(currentBuild)
            }
          }
        }
      }
    }
  }
}

#!/bin/bash
# Copyright 2021 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

readonly GIT_ROOT=$(git rev-parse --show-toplevel)

source "${GIT_ROOT}/scripts/deploy/common-setup.sh"
source "${GIT_ROOT}/scripts/deploy/resource-status.sh"

#
# Define common functions.
#
printhelp() {
  cat <<EOF
cluster-up.sh: Creates a Kubernetes cluster in the Azure cloud and install OpenEBS.
Options:
  -l, --location     [Required] : The region.
  -s, --subscription [Required] : The subscription identifier.
  -n, --name string             : The Kubernetes cluster DNS name. Defaults to
                                  k8s-<git-commit>-<template>.
  -o, --output string           : The output directory. Defaults to
                                  ./_output/<name>.
  -r, --resource-group string   : The resource group name. Defaults to 
                                  <name>-rg
  -t, --template string         : The cluster template name or URL. Defaults
                                  to multi-az.
  -v, --k8s-version string      : The Kubernetes version. Defaults to 1.21.
EOF
}

#
# Process the command line arguments.
#
unset AZURE_CLIENT_ID
unset AZURE_CLIENT_SECRET
unset AZURE_CLUSTER_DNS_NAME
unset AZURE_CLUSTER_TEMPLATE
unset AZURE_K8S_VERSION
unset AZURE_LOCATION
unset AZURE_RESOURCE_GROUP
unset AZURE_SUBSCRIPTION_ID
unset AZURE_TENANT_ID
unset ENABLE_AZURE_BASTION
unset OUTPUT_DIR
POSITIONAL=()

while [[ $# -gt 0 ]]
do
  ARG="$1"
  case $ARG in
    -d|--debug)
      set -x
      shift
      ;;

    -l|--location)
      AZURE_LOCATION="$2"
      shift 2 # skip the option arguments
      ;;

    -n|--name)
      AZURE_CLUSTER_DNS_NAME="$2"
      shift 2 # skip the option arguments
      ;;

    -o|--output)
      OUTPUT_DIR="$2"
      shift 2 # skip the option arguments
      ;;
    
    -r|--resource-group)
      AZURE_RESOURCE_GROUP="$2"
      shift 2 # skip the option arguments
      ;;

    -s|--subscription)
      AZURE_SUBSCRIPTION_ID="$2"
      shift 2 # skip the option arguments
      ;;

    -t|--template)
      AZURE_CLUSTER_TEMPLATE="$2"
      shift 2 # skip the option arguments
      ;;

    -v|--k8s-version)
      AZURE_K8S_VERSION="$2"
      shift 2 # skip the option arguments
      ;;

    -?|--help)
      printhelp
      exit 1
      ;;

    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters


#
# Validate command-line arguments and initialize variables.
#
if [[ ${#POSITIONAL[@]} -ne 0 ]]; then
  echoerr "ERROR: Unknown positional parameters - ${POSITIONAL[*]}"
  printhelp
  exit 1
fi

if [[ -z ${AZURE_SUBSCRIPTION_ID:-} ]]; then
  echoerr "ERROR: The --subscription option is required."
  printhelp
  exit 1
fi

if [[ -z ${AZURE_LOCATION:-} ]]; then
  echoerr "ERROR: The --location option is required."
  printhelp
  exit 1
fi

if [[ -z ${AZURE_CLUSTER_TEMPLATE:-} ]]; then
  AZURE_CLUSTER_TEMPLATE="multi-az"
fi

if [[ -z ${AZURE_K8S_VERSION:-} ]]; then
  AZURE_K8S_VERSION="1.21"
fi

IS_AZURE_CLUSTER_TEMPLATE_URI=$(expr "$(expr "${AZURE_CLUSTER_TEMPLATE}" : "file://\|https://\|http://")" != 0 || true)

if [[ ${IS_AZURE_CLUSTER_TEMPLATE_URI} -eq 0 ]]; then
  AZURE_CLUSTER_TEMPLATE_ROOT=${GIT_ROOT}/scripts/deploy
  AZURE_CLUSTER_TEMPLATE_FILE=${AZURE_CLUSTER_TEMPLATE_ROOT}/cluster/${AZURE_CLUSTER_TEMPLATE}/aks-config.json

  if [[ ! -f "$AZURE_CLUSTER_TEMPLATE_FILE" ]]; then
    AZURE_CLUSTER_VALID_TEMPLATES=$(find "${AZURE_CLUSTER_TEMPLATE_ROOT}" -maxdepth 1 -printf "%P\n" | awk '{split($1,f,"."); printf (NR>1?", ":"") f[1]}')
    echoerr "ERROR: The template '$AZURE_CLUSTER_TEMPLATE' is not known. Select one of the following values: $AZURE_CLUSTER_VALID_TEMPLATES."
    printhelp
    exit 1
  fi

  AZURE_CLUSTER_TEMPLATE_FILE=file://${AZURE_CLUSTER_TEMPLATE_FILE}
else
  AZURE_CLUSTER_TEMPLATE_FILE=${AZURE_CLUSTER_TEMPLATE}
fi

if [[ -z ${AZURE_CLUSTER_DNS_NAME:-} ]]; then
  CLUSTER_PREFIX=$(whoami)
  if [[ ${CLUSTER_PREFIX:-root} == "root" ]]; then
    CLUSTER_PREFIX=k8s
  fi
  AZURE_CLUSTER_DNS_NAME=$(basename "$(mktemp -t "${CLUSTER_PREFIX}-${AZURE_CLUSTER_TEMPLATE}-${GIT_COMMIT}-XXX")")
fi

if [[ -z ${AZURE_RESOURCE_GROUP:-} ]]; then
  AZURE_RESOURCE_GROUP=${AZURE_CLUSTER_DNS_NAME}-rg
fi

if [[ -z ${OUTPUT_DIR:-} ]]; then
  OUTPUT_DIR="$GIT_ROOT/_output/$AZURE_CLUSTER_DNS_NAME"
fi

#
# Install required tools
#
install_helm

#
# Create the Kubernetes cluster
#
echo "Creating cluster ${AZURE_CLUSTER_DNS_NAME}"
"${GIT_ROOT}/scripts/deploy/azure-cluster-up.sh" \
  --subscription "${AZURE_SUBSCRIPTION_ID}" \
  --location "${AZURE_LOCATION}" \
  --name "${AZURE_CLUSTER_DNS_NAME}" \
  --output "${OUTPUT_DIR}" \
  --k8s-version "${AZURE_K8S_VERSION}" \
  --template "${AZURE_CLUSTER_TEMPLATE}"

# Delete the cluster on subsequent errors.
trap_push "\"${OUTPUT_DIR}/cluster-down.sh\"" err

echo "Wait for cluster to become available..."
# TODO Figure out a better way to determine cluster availability
sleep 5m

export KUBECONFIG="${OUTPUT_DIR}/kubeconfig/kubeconfig.${AZURE_LOCATION}.json"

#
# Install the Azure Disk CSI Driver
#
# echo "Installing Azure Disk CSI Driver..."
# helm repo add azuredisk-csi-driver https://raw.githubusercontent.com/kubernetes-sigs/azuredisk-csi-driver/master/charts
# helm upgrade \
#   --install \
#   --namespace kube-system \
#   azuredisk-csi-driver \
#   azuredisk-csi-driver/azuredisk-csi-driver \
#   -f "${AZURE_CLUSTER_TEMPLATE_ROOT}/${AZURE_CLUSTER_TEMPLATE}/azuredisk-csi-driver-values.yaml"
# echo "Waiting for Azure Disk CSI Driver to start..."
# kubectl wait pods --namespace kube-system --selector app.kubernetes.io/instance=azuredisk-csi-driver --for condition=ready --timeout=15m

#
# Apply label to Mayastor Node Candidates
#
echo "Labelling Mayastor Node Candidates..."
kubectl label nodes --selector agentpool=agentpool openebs.io/engine=mayastor 

#
# Enable hugepages and prepare the nodes for reboot
#
echo "Enabling HugePages and marking all the nodes for reboot..."
kubectl apply -f actions/hugepage-enabler-daemonset.yaml
sleep 2m

#
# Restart nodes
#
echo "Restarting nodes..."
kubectl apply -f actions/kured-config.yaml
sleep 3m
getNodesStatus
echo "Nodes restart successful"

#
# Creating Mayastor Application Namespace and Resources
#
echo "Starting Mayastor specific operations..."
kubectl create namespace mayastor
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/operator-rbac.yaml
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/mayastorpoolcrd.yaml

#
# Deploy NATS
#
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/nats-deployment.yaml
sleep 2m
getPodsStatus "mayastor" "app=nats"

#
# Deploy dedicated etcd 
#
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/etcd/storage/localpv.yaml
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/etcd/statefulset.yaml 
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/etcd/svc.yaml
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/etcd/svc-headless.yaml
sleep 2m
getPodsStatus "mayastor" "app.kubernetes.io/name=etcd"

#
# Deploy Mayastor Components
#
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/csi-daemonset.yaml
sleep 2m
getDaemonsetStatus "mayastor" "mayastor-csi"

#
# Deploy Control Plane Components
#
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/core-agents-deployment.yaml
sleep 2m
getPodsStatus "mayastor" "app=core-agents"

kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/rest-deployment.yaml
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/rest-service.yaml
sleep 2m
getPodsStatus "mayastor" "app=rest"

kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/csi-deployment.yaml
sleep 2m
getPodsStatus "mayastor" "app=csi-controller"

kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor-control-plane/master/deploy/msp-deployment.yaml
sleep 2m
getPodsStatus "mayastor" "app=msp-operator"

#
# Deploy Data Plane
#
kubectl apply -f https://raw.githubusercontent.com/openebs/mayastor/master/deploy/mayastor-daemonset.yaml
sleep 2m
getDaemonsetStatus "mayastor" "mayastor"

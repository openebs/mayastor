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

#
# Define common functions.
#
printhelp() {
  cat <<EOF
azure-cluster-up.sh: Creates a Kubernetes cluster in the Azure cloud.

Options:
  -l, --location     [Required] : The region.
  -s, --subscription [Required] : The subscription identifier.
  -b, --enable-bastion          : If present, enable Azure Bastion for access
                                  to the cluster nodes.
  -c, --client-id string        : The service principal identifier. Default to
                                  <name>-sp.
  -e, --client-tenant string    : The client tenant. Required if --client-id
                                  is used.
  -n, --name string             : The Kubernetes cluster DNS name. Defaults to
                                  k8s-<git-commit>-<template>.
  -o, --output string           : The output directory. Defaults to
                                  ./_output/<name>.
  -p, --client-secret string    : The service principal password.  Required if
                                  --client-id is used.
  -r, --resource-group string   : The resource group name. Defaults to 
                                  <name>-rg
  -t, --template string         : The cluster template name or URL. Defaults
                                  to single-az.
  -v, --k8s-version string      : The Kubernetes version. Defaults to 1.17 for
                                  Linux and 1.18 for Windows.
EOF
}

echoerr() {
  printf "%s\n\n" "$*" >&2
}

trap_push() {
  local SIGNAL="${2:?Signal required}"
  HANDLERS="$( trap -p ${SIGNAL} | cut -f2 -d \' )";
  trap "${1:?Handler required}${HANDLERS:+;}${HANDLERS}" "${SIGNAL}"
}

retry() {
  local MAX_ATTEMPTS=$1
  local SLEEP_INTERVAL=$2
  shift 2

  local ATTEMPT=1
  while true;
  do
    ("$@") && break || true

    echoerr "Command '$1' failed $ATTEMPT of $MAX_ATTEMPTS attempts."

    ATTEMPT=$((ATTEMPT + 1))
    if [[ $ATTEMPT -ge $MAX_ATTEMPTS ]]; then
      return 1
    fi

    echoerr "Retry after $SLEEP_INTERVAL seconds..."
    sleep $SLEEP_INTERVAL
  done
}

choose() { echo ${1:RANDOM%${#1}:1} $RANDOM; }

genpwd() {
  PWD_SYMBOLS='~!@#$%^&*_-+=`|\(){}[]:;<>.?/'
  PWD_DIGITS='0123456789'
  PWD_LOWERCASE='abcdefghijklmnopqrstuvwxyz'
  PWD_UPPERCASE='ABCDEFGHIJKLMNOPQRSTUVWXYZ'
  PWD_ALL_CHARS="${PWD_DIGITS}${PWD_LOWERCASE}${PWD_UPPERCASE}${PWD_SYMBOLS}"

  {
      if [[ $1 -lt 4 ]]; then
          echoerr "ERROR: Password length must be at least 4"
          exit 1
      fi
      choose "${PWD_SYMBOLS}"
      choose "${PWD_DIGITS}"
      choose "${PWD_LOWERCASE}"
      choose "${PWD_UPPERCASE}"
      for i in $( seq 4 $1 )
      do
          choose "${PWD_ALL_CHARS}"
      done

  } | sort -R | awk '{printf "%s",$1}'
  echo ""
}

print_cleanup_command() {
  cat <<EOF
To delete the cluster, run the following command:

$ $1
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
    -b|--enable-bastion)
      ENABLE_AZURE_BASTION="true"
      shift
      ;;

    -c|--client-id)
      AZURE_CLIENT_ID="$2"
      shift 2 # skip the option arguments
      ;;

    -d|--debug)
      set -x
      shift
      ;;

    -e|--client-tenant)
      AZURE_TENANT_ID="$2"
      shift 2 # skip the option arguments
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
    
    -p|--client-secret)
      AZURE_CLIENT_SECRET="$2"
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
  echoerr "ERROR: Unknown positional parameters - ${POSITIONAL[@]}"
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

if [[ -n ${AZURE_CLIENT_ID:-} ]]; then
  if [[ -z ${AZURE_CLIENT_SECRET:-} ]]; then
    echoerr "ERROR: The --client-secret option is required when --client-id is used."
    printhelp
    exit 1
  fi
  if [[ -z ${AZURE_TENANT_ID:-} ]]; then
    echoerr "ERROR: The --client-tenant option is required when --client-id is used."
    printhelp
    exit 1
  fi
fi

if [[ -z ${AZURE_CLUSTER_TEMPLATE:-} ]]; then
  AZURE_CLUSTER_TEMPLATE="single-az"
fi

IS_AZURE_CLUSTER_TEMPLATE_URI=$(expr `expr "$AZURE_CLUSTER_TEMPLATE" : "https://\|http://"` != 0 || true)
IS_WINDOWS_CLUSTER=$(expr `expr "$AZURE_CLUSTER_TEMPLATE" : ".*-windows\|.*/windows-testing/.*"` != 0 || true)

if [[ -z ${AZURE_K8S_VERSION:-} ]]; then
  AZURE_K8S_VERSION="1.21"
fi

GIT_ROOT=$(git rev-parse --show-toplevel)
GIT_COMMIT=$(git rev-parse --short HEAD)

if [[ ${IS_AZURE_CLUSTER_TEMPLATE_URI} -eq 0 ]]; then
  AZURE_CLUSTER_TEMPLATE_ROOT=$GIT_ROOT/scripts/deploy
  AZURE_CLUSTER_TEMPLATE_FILE=${AZURE_CLUSTER_TEMPLATE_ROOT}/cluster/${AZURE_CLUSTER_TEMPLATE}/${AZURE_CLUSTER_TEMPLATE}.json

  if [[ ! -f "$AZURE_CLUSTER_TEMPLATE_FILE" ]]; then
    AZURE_CLUSTER_VALID_TEMPLATES=$(ls -x1 "$AZURE_CLUSTER_TEMPLATE_ROOT" | awk '{split($1,f,"."); printf (NR>1?", ":"") f[1]}')
    echoerr "ERROR: The template '$AZURE_CLUSTER_TEMPLATE' is not known. Select one of the following values: $AZURE_CLUSTER_VALID_TEMPLATES."
    printhelp
    exit 1
  fi
else
  AZURE_CLUSTER_TEMPLATE_FILE=$(mktemp -t "aks-engine-model-XXX.json")
  curl -sSfL "$AZURE_CLUSTER_TEMPLATE" -o "$AZURE_CLUSTER_TEMPLATE_FILE"
  AZURE_CLUSTER_TEMPLATE=$(basename -s ".json" "$AZURE_CLUSTER_TEMPLATE" | sed "s/_/-/g" )
fi

if [[ -z ${AZURE_CLUSTER_DNS_NAME:-} ]]; then
  CLUSTER_PREFIX=$(whoami)
  if [[ ${CLUSTER_PREFIX:-root} == "root" ]]; then
    CLUSTER_PREFIX=k8s
  fi
  # Generate the cluster name, replacing "windows" with "win" to keep ARM from preventing
  # creation of DNS name containing a trademarked named.
  AZURE_CLUSTER_DNS_NAME=$(basename "$(mktemp -t "${CLUSTER_PREFIX}-${AZURE_CLUSTER_TEMPLATE}-${GIT_COMMIT}-XXX")" | sed "s/windows/win/g")
fi

if [[ -z ${AZURE_RESOURCE_GROUP:-} ]]; then
  AZURE_RESOURCE_GROUP=${AZURE_CLUSTER_DNS_NAME}-rg
fi

AZURE_SUBSCRIPTION_URI="/subscriptions/${AZURE_SUBSCRIPTION_ID}"
AZURE_RESOURCE_GROUP_URI="${AZURE_SUBSCRIPTION_URI}/resourceGroups/${AZURE_RESOURCE_GROUP}"

if [[ -z ${OUTPUT_DIR:-} ]]; then
  OUTPUT_DIR="$GIT_ROOT/_output/$AZURE_CLUSTER_DNS_NAME"
fi

# Ensure the output directory exists.
mkdir -p $OUTPUT_DIR

#
# Install required tools if necessary.
#
if [[ -z "$(command -v az)" ]]; then
  echo "Installing Azure CLI..."
  curl -sSfL https://aka.ms/InstallAzureCLIDeb | sudo bash
fi

if [[ -z "$(command -v aks-engine)" ]]; then
  echo "Installing aks-engine..."
  curl -sSfL https://raw.githubusercontent.com/Azure/aks-engine/master/scripts/get-akse.sh | sudo bash -s -- --version v0.67.0
fi

#
# Login to Azure, and set up the service principal, resource group and cluster.
#
AZURE_ACTIVE_SUBSCRIPTION_ID=$(az account list --query="[?isDefault].id | [0]" --output=tsv || true)
if [[ -z $AZURE_ACTIVE_SUBSCRIPTION_ID ]]; then
  echo "Logging in to Azure..."
  az login 1> /dev/null
fi

if [[ "$AZURE_SUBSCRIPTION_ID" != "$AZURE_ACTIVE_SUBSCRIPTION_ID" ]]; then
  echo "Setting active subscription to $AZURE_SUBSCRIPTION_ID..."
  az account set --subscription ${AZURE_SUBSCRIPTION_ID} 1> /dev/null
fi

if [[ "$(az group exists --resource-group $AZURE_RESOURCE_GROUP)" != "true" ]]; then
  echo "Creating resource group $AZURE_RESOURCE_GROUP..."
  az group create --name $AZURE_RESOURCE_GROUP --location $AZURE_LOCATION 1> /dev/null
fi

CLEANUP_FILE="$OUTPUT_DIR/cluster-down.sh"
>$CLEANUP_FILE cat <<EOF
set -euo pipefail
AZURE_ACTIVE_SUBSCRIPTION_ID=\$(az account list --query="[?isDefault].id | [0]" --output=tsv)
if [[ -z \$AZURE_ACTIVE_SUBSCRIPTION_ID ]]; then
  echo "Logging in to Azure..."
  az login 1> /dev/null
fi
set -x
az group delete --subscription="$AZURE_SUBSCRIPTION_ID" --resource-group="$AZURE_RESOURCE_GROUP" --yes
EOF
chmod +x "$CLEANUP_FILE"

trap_push "print_cleanup_command \"${CLEANUP_FILE}\"" exit

# If no service principal was specified, create a new one or use the one from a previous
# run of this script.
if [[ -z ${AZURE_CLIENT_ID:-} ]]; then
  AZURE_CLIENT_NAME=${AZURE_CLUSTER_DNS_NAME}-sp
  AZURE_CLIENT_ID_FILE="$OUTPUT_DIR/$AZURE_CLIENT_NAME.id"
  AZURE_CLIENT_TENANT_FILE="$OUTPUT_DIR/$AZURE_CLIENT_NAME.tenant"
  AZURE_CLIENT_SECRET_FILE="$OUTPUT_DIR/$AZURE_CLIENT_NAME"

  if [[ -e "$AZURE_CLIENT_ID_FILE" ]] && [[ -e "$AZURE_CLIENT_SECRET_FILE" ]]; then
    echo "Using existing service principal $AZURE_CLIENT_NAME..."
    AZURE_TENANT_ID=$(cat "$AZURE_CLIENT_TENANT_FILE")
    AZURE_CLIENT_ID=$(cat "$AZURE_CLIENT_ID_FILE")
    AZURE_CLIENT_SECRET=$(cat "$AZURE_CLIENT_SECRET_FILE")

    echo "Assigning $AZURE_CLIENT_ID to \"Owner\" role of $AZURE_RESOURCE_GROUP..."
    az role assignment create \
        --assignee=$AZURE_CLIENT_ID \
        --role=Owner \
        --resource-group=$AZURE_RESOURCE_GROUP 1> /dev/null
  else
    echo "Creating service principal $AZURE_CLIENT_NAME..."
    AZURE_CLIENT_INFO=($(az ad sp create-for-rbac \
      --name="http://$AZURE_CLIENT_NAME" \
      --role="Contributor" \
      --scopes="$AZURE_SUBSCRIPTION_URI" \
      --output=tsv \
      --query="[tenant,appId,password]"))
    AZURE_TENANT_ID=${AZURE_CLIENT_INFO[0]}
    AZURE_CLIENT_ID=${AZURE_CLIENT_INFO[1]}
    AZURE_CLIENT_SECRET=${AZURE_CLIENT_INFO[2]}
    unset AZURE_CLIENT_INFO

    echo "$AZURE_TENANT_ID" > "$AZURE_CLIENT_TENANT_FILE"
    echo "$AZURE_CLIENT_ID" > "$AZURE_CLIENT_ID_FILE"
    echo "$AZURE_CLIENT_SECRET" > "$AZURE_CLIENT_SECRET_FILE"
  fi

  # Add removal of the service principal to the cleanup script.
  echo "az ad sp delete --id=$AZURE_CLIENT_ID" >> $CLEANUP_FILE

  echo "Waiting for service principal to become available in directory..."
  trap_push 'az logout &> /dev/null' exit
  retry 5 60 az login \
      --service-principal \
      --tenant="$AZURE_TENANT_ID" \
      --username="$AZURE_CLIENT_ID" \
      --password="$AZURE_CLIENT_SECRET" \
      --allow-no-subscriptions 1> /dev/null
fi

AZURE_ADMIN_PASSWORD_FILE="$OUTPUT_DIR/id_azureuser.pwd"
AZURE_ADMIN_PRIVATE_KEY_FILE="$OUTPUT_DIR/id_azureuser"
AZURE_ADMIN_PUBLIC_KEY_FILE="$AZURE_ADMIN_PRIVATE_KEY_FILE.pub"

if [[ $IS_WINDOWS_CLUSTER -ne 0 ]] && [[ ! -e "$AZURE_ADMIN_PASSWORD_FILE" ]]; then
  echo "Creating Windows administrator password..."
  genpwd 16 > "$AZURE_ADMIN_PASSWORD_FILE" || true
fi

if [[ ! -e "$AZURE_ADMIN_PUBLIC_KEY_FILE" ]]; then
  echo "Creating administrator SSH key pair..."
  ssh-keygen -q -t rsa -b 4096 -f "$AZURE_ADMIN_PRIVATE_KEY_FILE" -N ""
fi

AZURE_ADMIN_PUBLIC_KEY=$(cat "$AZURE_ADMIN_PUBLIC_KEY_FILE")

echo "Creating Kubernetes cluster $AZURE_CLUSTER_DNS_NAME..."
API_MODEL="$OUTPUT_DIR/$AZURE_CLUSTER_DNS_NAME-model.json"

cat "$AZURE_CLUSTER_TEMPLATE_FILE" | \
  sed "s/\"location\": \".*\"/\"location\": \"$AZURE_LOCATION\"/" > $API_MODEL

AKS_ENGINE_OVERRIDES=(\
  --set orchestratorProfile.orchestratorRelease="$AZURE_K8S_VERSION" \
  --set orchestratorProfile.kubernetesConfig.useManagedIdentity=false \
  --set masterProfile.dnsPrefix="$AZURE_CLUSTER_DNS_NAME" \
  --set linuxProfile.ssh.publicKeys[0].keyData="$AZURE_ADMIN_PUBLIC_KEY" \
  --set servicePrincipalProfile.clientID="$AZURE_CLIENT_ID" \
  --set servicePrincipalProfile.secret="$AZURE_CLIENT_SECRET")

if [[ $IS_WINDOWS_CLUSTER -ne 0 ]]; then
  AZURE_ADMIN_PASSWORD=$(cat "$AZURE_ADMIN_PASSWORD_FILE")
  AKS_ENGINE_OVERRIDES+=(--set windowsProfile.adminPassword="$AZURE_ADMIN_PASSWORD")
fi

aks-engine deploy \
  --subscription-id=$AZURE_SUBSCRIPTION_ID \
  --resource-group=$AZURE_RESOURCE_GROUP \
  --location=$AZURE_LOCATION \
  --client-id=$AZURE_CLIENT_ID \
  --client-secret="$AZURE_CLIENT_SECRET" \
  --api-model="$API_MODEL" \
  --output-directory="$OUTPUT_DIR" \
  --force-overwrite \
  "${AKS_ENGINE_OVERRIDES[@]}"

# Set up Bastion access if requested
if [[ ${ENABLE_AZURE_BASTION:-false} == "true" ]]; then
  ${GIT_ROOT}/scripts/deploy/enable-azure-bastion.sh \
    --subscription "$AZURE_SUBSCRIPTION_ID" \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --location "$AZURE_LOCATION" \
    --name "$AZURE_CLUSTER_DNS_NAME"
fi

AZURE_CLUSTER_KUBECONFIG_FILE="$OUTPUT_DIR/kubeconfig/kubeconfig.$AZURE_LOCATION.json"
echo "Waiting for cluster to become available"
retry 30 10 kubectl --kubeconfig="$AZURE_CLUSTER_KUBECONFIG_FILE" get nodes 1> /dev/null

#
# Create setup and clean-up shell scripts.
#
SETUP_FILE="$OUTPUT_DIR/e2e-setup.sh"
>$SETUP_FILE cat <<EOF
export ARTIFACTS="$OUTPUT_DIR/artifacts"
export AZURE_TENANT_ID="$AZURE_TENANT_ID"
export AZURE_SUBSCRIPTION_ID="$AZURE_SUBSCRIPTION_ID"
export AZURE_CLIENT_ID="$AZURE_CLIENT_ID"
export AZURE_CLIENT_SECRET="$AZURE_CLIENT_SECRET"
export AZURE_RESOURCE_GROUP="$AZURE_RESOURCE_GROUP"
export AZURE_LOCATION="$AZURE_LOCATION"
export KUBECONFIG="$AZURE_CLUSTER_KUBECONFIG_FILE"
EOF

if [[ ${IS_WINDOWS_CLUSTER} -ne 0 ]]; then
  >>$SETUP_FILE echo "export TEST_WINDOWS=\"true\""
fi

chmod +x "$SETUP_FILE"

mkdir -p "$OUTPUT_DIR/artifacts"

echo
echo "To use the $AZURE_CLUSTER_DNS_NAME cluster, set KUBECONFIG to the following:"
echo
echo "$ export KUBECONFIG=\"$AZURE_CLUSTER_KUBECONFIG_FILE\""
echo
echo "To setup for e2e tests, run the following command:"
echo
echo "$ source \"$SETUP_FILE\""
echo

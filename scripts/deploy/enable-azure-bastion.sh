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
enable-azure-bastion.sh: Enables Azure Bastion on a cluster created using
  azure-cluster-up.sh.

Options:
  -l, --location       [Required] : The region.
  -r, --resource-group [Required] : The resource group name.
  -s, --subscription   [Required] : The subscription identifier.
  -n, --name           [Required] : The Kubernetes cluster DNS name.
EOF
}

echoerr() {
  printf "%s\n\n" "$*" >&2
}

#
# Process the command line arguments.
#
unset AZURE_CLUSTER_DNS_NAME
unset AZURE_LOCATION
unset AZURE_RESOURCE_GROUP
unset AZURE_SUBSCRIPTION_ID
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

    -r|--resource-group)
      AZURE_RESOURCE_GROUP="$2"
      shift 2 # skip the option arguments
      ;;

    -s|--subscription)
      AZURE_SUBSCRIPTION_ID="$2"
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

if [[ -z ${AZURE_RESOURCE_GROUP:-} ]]; then
  echoerr "ERROR: The --resource-group option is required."
  printhelp
  exit 1
fi

if [[ -z ${AZURE_CLUSTER_DNS_NAME:-} ]]; then
  echoerr "ERROR: The --name option is required."
  printhelp
  exit 1
fi

#
# Login to Azure and setup Bastion.
#
AZURE_ACTIVE_SUBSCRIPTION_ID=$(az account list --query="[?isDefault].id | [0]" --output=tsv || true)
if [[ -z $AZURE_ACTIVE_SUBSCRIPTION_ID ]]; then
  echo "Logging in to Azure..."
  az login 1> /dev/null
fi

AZURE_BASTION_DNS_NAME="${AZURE_CLUSTER_DNS_NAME}-bastion"
AZURE_BASTION_NSG_NAME="${AZURE_BASTION_DNS_NAME}-nsg"

echo "Setting up network security for Bastion access..."
az network nsg create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --location="$AZURE_LOCATION" \
  --name="$AZURE_BASTION_NSG_NAME" 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowHttpInbound \
  --priority=120 \
  --access=Allow \
  --direction=Inbound \
  --protocol=Tcp \
  --source-address-prefixes=Internet \
  --source-port-ranges='*' \
  --destination-address-prefixes='*' \
  --destination-port-ranges=443 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowGatewayManagerInbound \
  --priority=130 \
  --access=Allow \
  --direction=Inbound \
  --protocol=Tcp \
  --source-address-prefixes=GatewayManager \
  --source-port-ranges='*' \
  --destination-address-prefixes='*' \
  --destination-port-ranges=443 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowLoadBalancerInbound \
  --priority=140 \
  --access=Allow \
  --direction=Inbound \
  --protocol=Tcp \
  --source-address-prefixes=AzureLoadBalancer \
  --source-port-ranges='*' \
  --destination-address-prefixes='*' \
  --destination-port-ranges=443 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowBastionHostCommunication \
  --priority=150 \
  --access=Allow \
  --direction=Inbound \
  --protocol='*' \
  --source-address-prefixes=VirtualNetwork \
  --source-port-ranges='*' \
  --destination-address-prefixes='*' \
  --destination-port-ranges 8080 5701 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowSshRdpOutbound \
  --priority=100 \
  --access=Allow \
  --direction=Outbound \
  --protocol='*' \
  --source-address-prefixes='*' \
  --source-port-ranges='*' \
  --destination-address-prefixes=VirtualNetwork \
  --destination-port-ranges 22 3389 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowAzureCloudOutbound \
  --priority=110 \
  --access=Allow \
  --direction=Outbound \
  --protocol=Tcp \
  --source-address-prefixes='*' \
  --source-port-ranges='*' \
  --destination-address-prefixes=AzureCloud \
  --destination-port-ranges 443 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowBastionCommunication \
  --priority=120 \
  --access=Allow \
  --direction=Outbound \
  --protocol='*' \
  --source-address-prefixes=VirtualNetwork \
  --source-port-ranges='*' \
  --destination-address-prefixes=VirtualNetwork \
  --destination-port-ranges 8080 5701 1> /dev/null
az network nsg rule create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --nsg-name="$AZURE_BASTION_NSG_NAME" \
  --name=AllowGuestSessionInformation \
  --priority=130 \
  --access=Allow \
  --direction=Outbound \
  --protocol='*' \
  --source-address-prefixes='*' \
  --source-port-ranges='*' \
  --destination-address-prefixes=Internet \
  --destination-port-ranges 80 1> /dev/null

echo "Setting up virtual network and public IP address for Bastion access..."
AZURE_CLUSTER_NETWORK=$(az network vnet list \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --query='[].{name:name,addressPrefix:subnets[*].addressPrefix}|[?starts_with(@.addressPrefix[0],`10.240.0.0`)].name|[0]' \
  --output=tsv)
if [[ -z ${AZURE_CLUSTER_NETWORK:-} ]]; then
  echoerr ERROR: Failed to find a suitable virtual network.
  exit 1
fi

az network vnet subnet create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --vnet-name="$AZURE_CLUSTER_NETWORK" \
  --name=AzureBastionSubnet \
  --address-prefixes="10.224.0.0/27" \
  --network-security-group="$AZURE_BASTION_NSG_NAME" 1> /dev/null

az network public-ip create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --location="$AZURE_LOCATION" \
  --name="$AZURE_BASTION_DNS_NAME" \
  --sku Standard 1> /dev/null

echo "Adding Bastion access to cluster..."
az network bastion create \
  --subscription="$AZURE_SUBSCRIPTION_ID" \
  --resource-group="$AZURE_RESOURCE_GROUP" \
  --location="$AZURE_LOCATION" \
  --vnet-name="$AZURE_CLUSTER_NETWORK" \
  --name="$AZURE_BASTION_DNS_NAME" \
  --public-ip-address="$AZURE_BASTION_DNS_NAME" 1> /dev/null

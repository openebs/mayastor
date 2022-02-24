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

install_helm() {
	if [[ -z "$(command -v helm)" ]]; then
		echo "Installing helm..."
		curl https://raw.githubusercontent.com/helm/master/scripts/get-helm-3 | bash
	fi
}

install_aksengine() {
	if [[ -z "$(command -v aks-engine)" ]]; then
		echo "Installing aks-engine..."
		curl -sSfL https://raw.githubusercontent.com/Azure/aks-engine/master/scripts/get-akse.sh | sudo bash
	fi
}

install_azurecli() {
	if [[ -z "$(command -v az)" ]]; then
		echo "Installing Azure CLI..."
		curl -sSfL https://aka.ms/InstallAzureCLIDeb | sudo bash
	fi
}

echoerr() {
  printf "%s\n\n" "$*" >&2
}

trap_push() {
  local SIGNAL="${2:?Signal required}"
  HANDLERS="$( trap -p ${SIGNAL} | cut -f2 -d \' )";
  trap "${1:?Handler required}${HANDLERS:+;}${HANDLERS}" "${SIGNAL}"
}

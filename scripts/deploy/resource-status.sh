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

getPodsStatus() {
	namespace="$1"
	selector="$2"
	# We will try for 5 mins
	end=$(( SECONDS + 5*60 ))
	running=0

	while [ $SECONDS -lt $end ]; do
		val=$(kubectl get pods -n $namespace --selector=$selector)
		IFS=$'\n'
		rows=($val)
		podCount=$(( ${#rows[@]} - 1 ))
		runningCount=0
		for (( i=1; i<${#rows[@]}; i++ )); do
			IFS=' '
			row=${rows[i]}
			rowSplit=($row)
			status=${rowSplit[2]}
			if [ "$status" = "Running" ]; then
				((runningCount=runningCount+1))
			fi
		done

		if [ $podCount -eq $runningCount ]; then
			running=1
			break
		fi
		sleep 1s
	done

	if [ $running -eq 1 ]; then
		return 0
	fi

	return 1
}

function getDaemonsetStatus() {
	namespace="$1"
	daemonsetName="$2"
	# We will try for 5 mins
	end=$(( SECONDS + 5*60 ))
	running=0

	while [ $SECONDS -lt $end ]; do
		val=$(kubectl get daemonset -n $namespace $daemonsetName)
		IFS=$'\n'
		rows=($val)
		if [ ${#rows[@]} -gt 1 ]; then
			daemonsetRow=${rows[1]}
			IFS=' '
			rowSplit=($daemonsetRow)
			if [ ${rowSplit[1]} -eq ${rowSplit[3]} ]; then
				running=1
				break
			fi
		fi
		sleep 1s
	done

	if [ $running -eq 1 ]; then
		return 0
	fi

	return 1
}

getNodesStatus() {
	# We will try for 15 mins
	end=$(( SECONDS + 15*60 ))
	ready=0
	echo "Waiting for 15 mins for all the nodes to return to Ready state..."
	while [ $SECONDS -lt $end ]; do
		val=$(kubectl get nodes)
		IFS=$'\n'
		rows=($val)
		nodeCount=$(( ${#rows[@]} - 1 ))
		ready=0
		for (( i=1; i<=nodeCount; i++ )); do
			IFS=' '
			row=${rows[i]}
			rowSplit=($row)
			if [ "${rowSplit[1]}" = "Ready" ]; then
				((ready=ready+1))
			fi
		done

		if [ $nodeCount -eq $ready ]; then
			ready=1
			break
		fi
		sleep 1s
	done

	if [ $ready -eq 1 ]; then
		return 0
	fi

	return 1
}

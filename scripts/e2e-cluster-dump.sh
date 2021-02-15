#!/usr/bin/env bash

# This script makes the best attempt to dump stuff
# so ignore fails and keep paddling.
# set -e

help() {
  cat <<EOF
This script generates logs for mayastor pods and cluster state.

Usage: $0 [OPTIONS]

Options:
  --destdir <path>   Location to store log files
  --clusteronly   Only generate cluster information

If --destdir is not specified the data is dumped to stdout
EOF
}

function cluster-get {
    echo "-- PODS mayastor* --------------------"
    # The CSI tests creates namespaces containing the text mayastor
    mns=$(kubectl get ns | grep mayastor | sed -e "s/ .*//")
    for ns in $mns
    do
        kubectl -n "$ns" -o wide get pods --sort-by=.metadata.creationTimestamp
    done
    echo "-- PODS ------------------------------"
    kubectl get -o wide pods --sort-by=.metadata.creationTimestamp
    echo "-- PVCS ------------------------------"
    kubectl get pvc --sort-by=.metadata.creationTimestamp
    echo "-- PV --------------------------------"
    kubectl get pv --sort-by=.metadata.creationTimestamp
    echo "-- Storage Classes -------------------"
    kubectl get sc --sort-by=.metadata.creationTimestamp
    echo "-- Mayastor Pools --------------------"
    kubectl -n mayastor get msp --sort-by=.metadata.creationTimestamp
    echo "-- Mayastor Volumes ------------------"
    kubectl -n mayastor get msv --sort-by=.metadata.creationTimestamp
    echo "-- Mayastor Nodes --------------------"
    kubectl -n mayastor get msn --sort-by=.metadata.creationTimestamp
    echo "-- K8s Nodes -----------------------------"
    kubectl get nodes -o wide --show-labels
    echo "-- K8s Deployments -------------------"
    kubectl -n mayastor get deployments
    echo "-- K8s Daemonsets --------------------"
    kubectl -n mayastor get daemonsets

}

function cluster-describe {
    echo "-- PODS mayastor* --------------------"
    # The CSI tests creates namespaces containing the text mayastor
    mns=$(kubectl get ns | grep mayastor | sed -e "s/ .*//")
    for ns in $mns
    do
        kubectl -n "$ns" describe pods
    done
    echo "-- PODS ------------------------------"
    kubectl describe pods
    echo "-- PVCS ------------------------------"
    kubectl describe pvc
    echo "-- PV --------------------------------"
    kubectl describe pv
    echo "-- Storage Classes -------------------"
    kubectl describe sc
    echo "-- Mayastor Pools --------------------"
    kubectl -n mayastor describe msp
    echo "-- Mayastor Volumes ------------------"
    kubectl -n mayastor describe msv
    echo "-- Mayastor Nodes --------------------"
    kubectl -n mayastor describe msn
    echo "-- K8s Nodes -------------------------"
    kubectl describe nodes
    echo "-- K8s Deployments -------------------"
    kubectl -n mayastor describe deployments
    echo "-- K8s Daemonsets --------------------"
    kubectl -n mayastor describe daemonsets
}

function podHasRestarts {
    rst=$(kubectl -n mayastor get pods "$1" | grep -v NAME | awk '{print $4}')

    # Adjust the return value, to yield readable statements, like:
    # if podHasRestarts $podname ; then
    #     handle_restarted_pods
    # fi
    if [ $((rst)) -ne 0 ]; then
        return 0
    else
        return 1
    fi
}

# args filename kubectlargs
# filename == "" -> stdout
function kubectlEmitLogs {
    fname=$1
    shift

    if [ -n "$fname" ]; then
        kubectl -n mayastor logs "$@" >& "$fname"
    else
        kubectl -n mayastor logs "$@"
    fi
}

# args = destdir podname containername
# if $destdir != "" then log files are generate in $destdir
#   with the name of the pod and container.
function emitPodContainerLogs {
    destdir=$1
    podname=$2
    containername=$3

    if [ -z "$podname" ] || [ -z "$containername" ]; then
        echo "ERROR calling emitPodContainerLogs"
        return
    fi

    if podHasRestarts "$podname" ; then
        if [ -z "$destdir" ]; then
            echo "# $podname $containername previous -------------------"
            logfile=""
        else
            logfile="$destdir/$podname.$containername.previous.log"
        fi

        kubectlEmitLogs "$logfile" -p "$podname" "$containername"
    fi

    if [ -z "$destdir" ]; then
        echo "# $podname $containername ----------------------------"
        logfile=""
    else
        logfile="$destdir/$podname.$containername.log"
    fi

    kubectlEmitLogs "$logfile" "$podname" "$containername"
}

# arg1 = destdir or "" for stdout
function getLogsMayastorCSI {
    mayastor_csipods=$(kubectl -n mayastor get pods | grep mayastor-csi | sed -e 's/ .*//')
    for pod in $mayastor_csipods
    do
        # emitPodContainerLogs destdir podname containername
        emitPodContainerLogs "$1" "$pod" mayastor-csi
        emitPodContainerLogs "$1" "$pod" csi-driver-registrar
    done
}

# arg1 = destdir or "" for stdout
function getLogsMayastor {
    mayastor_pods=$(kubectl -n mayastor get pods | grep mayastor | grep -v mayastor-csi | sed -e 's/ .*//')
    for pod in $mayastor_pods
    do
        # emitPodContainerLogs destdir podname containername
        emitPodContainerLogs "$1" "$pod" mayastor
    done
}

# arg1 = destdir or "" for stdout
function getLogsMOAC {
    moacpod=$(kubectl -n mayastor get pods | grep moac | sed -e 's/ .*//')
    # emitPodContainerLogs destdir podname containername
    emitPodContainerLogs "$1" "$moacpod" moac
    emitPodContainerLogs "$1" "$moacpod" csi-provisioner
    emitPodContainerLogs "$1" "$moacpod" csi-attacher
}

# $1 = podlogs, 0 => do not generate pod logs
# $2 = [destdir] undefined => dump to stdout,
#                   otherwise generate log files in $destdir
function getLogs {
    podlogs="$1"
    shift
    dest="$1"
    shift

    if [ -n "$dest" ];
    then
        mkdir -p "$dest"
    fi

    if [ "$podlogs" -ne 0 ]; then
        getLogsMOAC "$dest"
        getLogsMayastor "$dest"
        getLogsMayastorCSI "$dest"
    fi

    if [ -n "$dest" ];
    then
        cluster-get >& "$dest/cluster.get.txt"
        cluster-describe >& "$dest/cluster.describe.txt"

        echo "logfiles generated in $dest"
        ls -l "$dest"
        echo ""

    else
        cluster-get
        cluster-describe
    fi
}

podlogs=1
destdir=

# Parse arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d|--destdir)
      shift
      destdir="$1"
      ;;
    -c|--clusteronly)
      podlogs=0
      ;;
    *)
      echo "Unknown option: $1"
      help
      exit 1
      ;;
  esac
  shift
done

# getLogs podlogs destdir
getLogs "$podlogs" "$destdir"

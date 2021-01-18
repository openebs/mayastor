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
    # csi tests creates relevant namespaces containing mayastor
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
}

function cluster-describe {
    echo "-- PODS mayastor* --------------------"
    # csi tests creates relevant namespaces containing mayastor
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
    echo "-- K8s Nodes -----------------------------"
    kubectl describe nodes
}

function logs-csi-containers {
    mayastor_csipods=$(kubectl -n mayastor get pods | grep mayastor-csi | sed -e 's/ .*//')
    for pod in $mayastor_csipods
    do
        echo "# $pod csi-driver-registrar $* ---------------------------------"
        kubectl -n mayastor logs "$@" "$pod" csi-driver-registrar
    done

    moacpod=$(kubectl -n mayastor get pods | grep moac | sed -e 's/ .*//')
    echo "# $moacpod csi-provisioner $* ---------------------------------"
    kubectl -n mayastor logs "$@" "$moacpod" csi-provisioner
    echo "# $moacpod csi-attacher $* ---------------------------------"
    kubectl -n mayastor logs "$@" "$moacpod" csi-attacher
}

function logs-csi-mayastor {
    mayastor_csipods=$(kubectl -n mayastor get pods | grep mayastor-csi | sed -e 's/ .*//')
    for pod in $mayastor_csipods
    do
        echo "# $pod mayastor-csi $* ---------------------------------"
        kubectl -n mayastor logs "$@" "$pod"  mayastor-csi
    done
}

function logs-mayastor {
    mayastor_pods=$(kubectl -n mayastor get pods | grep mayastor | grep -v mayastor-csi | sed -e 's/ .*//')
    for pod in $mayastor_pods
    do
        echo "# $pod mayastor $* ---------------------------------"
        kubectl -n mayastor logs "$@" "$pod"  mayastor
    done
}

function logs-moac {
    moacpod=$(kubectl -n mayastor get pods | grep moac | sed -e 's/ .*//')
    echo "# $moacpod moac $* ---------------------------------"
    kubectl -n mayastor logs "$@" "$moacpod" moac
}

# $1 = podlogs, 0 => do not generate pod logs
function dump-to-stdout {
    echo "# Cluster ---------------------------------"
    cluster-get
    cluster-describe

    if [ "$1" -ne 0 ]; then
        logs-moac
        logs-mayastor
        logs-csi-mayastor
        logs-csi-containers

        logs-moac -p
        logs-mayastor -p
        logs-csi-mayastor -p
        logs-csi-containers -p
    fi
    echo "# END ---------------------------------"
}

# $1 = podlogs, 0 => do not generate pod logs
# $2 = dest  mkdir $dest and generate logs there.
function dump-to-dir {
    dest="$2"
    echo "Generating logs in $dest"
    mkdir -p "$dest"

    cluster-get >& "$dest/cluster.get.txt"
    cluster-describe >& "$dest/cluster.describe.txt"

    if [ "$1" -ne 0 ]; then
        logs-moac >& "$dest/moac.log"
        logs-mayastor >& "$dest/mayastor.log"
        logs-csi-mayastor >& "$dest/csi-mayastor.log"
        logs-csi-containers >& "$dest/csi-containers.log"

        logs-moac -p >& "$dest/moac.previous.log"
        logs-mayastor -p >& "$dest/mayastor.previous.log"
        logs-csi-mayastor -p >& "$dest/csi-mayastor.previous.log"
        logs-csi-containers -p >& "$dest/csi-containers.previous.log"
    fi
}

# $1 = podlogs, 0 => do not generate pod logs
# $2 = [destdir] undefined => dump to stdout,
#                   otherwise generate log files in $destdir
function dump {
    if [ -z "$2" ]; then
        dump-to-stdout "$1"
    else
        dump-to-dir "$1" "$2"
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

# @here dump to stdout
dump "$podlogs" "$destdir"

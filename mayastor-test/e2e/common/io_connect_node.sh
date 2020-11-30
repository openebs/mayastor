#!/usr/bin/env bash

set -e

# Script to disconnect a node from another node using iptables
# $1 is the node-name to isolate/restore
# $2 is the other node-name
# $3 is "DISCONNECT" or "RECONNECT"
# assumes the nodes use a known fixed set of IP addresses and node names

# edit the line below, if necessary, or set KUBESPRAY_REPO when calling
KUBESPRAY_REPO="${KUBESPRAY_REPO:-$HOME/work/kubespray}"

if [ $# -ne 3 ];
    then echo "specify node-name, other node-name and action (DISCONNECT or RECONNECT)"
    exit 1
fi

if [ "$3" = "DISCONNECT" ]; then
	action="I"
elif [ "$3" = "RECONNECT" ]; then
	action="D"
else
    echo "specify action (DISCONNECT or RECONNECT)"
    exit 1
fi

cd ${KUBESPRAY_REPO}

nodename=$1
other_nodename=$2
other_node_suffix=${other_nodename: -1}
other_ip=172.18.8.10${other_node_suffix}

# apply the rule to block/unblock it
vagrant ssh ${nodename} -c "sh -c 'sudo iptables -${action} INPUT -s ${other_ip} -j REJECT'"
vagrant ssh ${nodename} -c "sh -c 'sudo iptables -${action} OUTPUT -s ${other_ip} -j REJECT'"


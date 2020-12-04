#!/usr/bin/env bash

set -e

# Script to disconnect a node from another node using iptables
# $1 is the hostname of the node to change
# $2 is the target IP address of the connection to change
# $3 is "DISCONNECT" or "RECONNECT"
# $4 is "DROP" or "REJECT"

# edit the line below, if necessary, or set KUBESPRAY_REPO when calling
KUBESPRAY_REPO="${KUBESPRAY_REPO:-$HOME/work/kubespray}"

if [ $# -ne 4 ];
    then echo "specify node-name, target node-ip-address, action (DISCONNECT or RECONNECT), and (DROP or REJECT)"
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

if [ "$4" != "DROP" ] && [ "$4" != "REJECT" ]; then
    echo "specify DROP or REJECT"
    exit 1
fi


cd ${KUBESPRAY_REPO}

node_name=$1
other_ip=$2

# apply the rule to block/unblock it
vagrant ssh ${node_name} -c "sh -c 'sudo iptables -${action} INPUT -s ${other_ip} -j $4'"
vagrant ssh ${node_name} -c "sh -c 'sudo iptables -${action} OUTPUT -s ${other_ip} -j $4'"


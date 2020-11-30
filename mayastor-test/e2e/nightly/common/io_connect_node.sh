#!/usr/bin/env bash

# Script to disconnect a node from another node using iptables
# $1 is the node-name to isolate/restore
# $2 is the other node-name
# $3 is "I" to disconnect, "D" to reconnect

# edit the line below, if necessary
KUBESPRAY_REPO="$HOME/work/kubespray"

if [ $# -ne 3 ];
    then echo "specify node-name, other node-name  and action (D or I)"
    exit 1
fi

cd ~/work/kubespray

nodename=$1
other_nodename=$2
other_node_suffix=${other_nodename: -1}

other_ip=172.18.8.10$((other_node_suffix))

# apply the rule to block/unblock it
vagrant ssh ${nodename} -c "sudo sh -c 'sudo iptables -$3 INPUT -s ${other_ip} -j REJECT'"
vagrant ssh ${nodename} -c "sudo sh -c 'sudo iptables -$3 OUTPUT -s ${other_ip} -j REJECT'"


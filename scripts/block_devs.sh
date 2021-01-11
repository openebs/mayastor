#!/bin/env bash

for i in {0..7}
do
	truncate /tmp/"${i}".blk -s 1G
done;

modprobe nvmet_tcp

mkdir /sys/kernel/config/nvmet/ports/1
cd /sys/kernel/config/nvmet/ports/1 || exit
echo 127.0.0.1 > addr_traddr
echo tcp > addr_trtype
echo 4420 > addr_trsvcid
echo ipv4 > addr_adrfam

for instance in {0..7}
do
	mkdir /sys/kernel/config/nvmet/subsystems/replica"${instance}"
	cd /sys/kernel/config/nvmet/subsystems/replica"${instance}" || exit
	echo 1 > attr_allow_any_host
	mkdir namespaces/1
	cd namespaces/1 || exit
	echo -n /tmp/"${instance}".blk > device_path
	echo 1 > enable
	ln -s /sys/kernel/config/nvmet/subsystems/replica"${instance}" /sys/kernel/config/nvmet/ports/1/subsystems/replica"${instance}"
done;

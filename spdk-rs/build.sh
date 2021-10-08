#!/bin/env bash
# This script replicates the build of libspdk as its done by nix without setting
# any specifics in terms of CPU. The purpose is to easily make changes to libspdk
# locally and then recompile it and test it with mayastor.
#

pushd spdk || { echo "Can not find spdk directory"; exit; }
rm libspdk.so
[ ! -d dpdk/.git ] || { echo "Submodules not checked out?"; exit; }


./configure --enable-debug \
	--target-arch=nehalem \
	--without-isal \
	--with-crypto \
	--with-uring \
	--disable-unit-tests \
	--disable-tests \
	--with-fio=$(which fio | sed s';bin/fio;include;')

#bear -- make -j 8
make -j $(nproc)
# delete things we for sure do not want link
find . -type f -name 'libspdk_event_nvmf.a' -delete
find . -type f -name 'libspdk_sock_uring.a' -delete
find . -type f -name 'libspdk_ut_mock.a' -delete
find . -type f -name 'libspdk_bdev_blobfs.a' -delete
find . -type f -name 'libspdk_bdev_ftl.a' -delete
find . -type f -name 'libspdk_bdev_gpt.a' -delete
find . -type f -name 'libspdk_bdev_passthru.a' -delete
find . -type f -name 'libspdk_bdev_raid.a' -delete
find . -type f -name 'libspdk_bdev_split.a' -delete
find . -type f -name 'libspdk_bdev_zone_block.a' -delete

# the event libraries are the libraries that parse configuration files
# we do our own config file parsing, and we setup our own targets.

$CC -shared -o libspdk.so \
	-lc  -laio -lnuma -ldl -lrt -luuid -lpthread -lcrypto \
	-luring \
	-Wl,--whole-archive \
	$(find build/lib -type f -name 'libspdk_*.a*' -o -name 'librte_*.a*') \
	$(find dpdk/build/lib -type f -name 'librte_*.a*') \
	$(find intel-ipsec-mb -type f -name 'libIPSec_*.a*') \
	-Wl,--no-whole-archive

echo "libspdk.so located in $(pwd)"
popd || exit

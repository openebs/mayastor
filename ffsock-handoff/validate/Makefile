# Makefile — Phase 2 harness (F-Stack embedded under SPDK's EAL + reactor).
#
# Unlike the spike, this links the REAL SPDK env (libspdk_env_dpdk) and the REAL
# DPDK — there is ONE EAL, owned by SPDK. F-Stack (libfstack) rides on it and is
# reached through the ffsock module (libspdk_sock_ff). net_tap + gso must be
# present in SPDK's DPDK build (see spdk/dpdkbuild/Makefile: DPDK_DRIVERS/LIBS).

SPDK_PATH ?= /root/mayastore/spdk
FF_PATH   ?= /root/mayastore/f-stack
DPDK_PC   := $(SPDK_PATH)/dpdk/build/lib/pkgconfig

CC        := gcc
CFLAGS    := -O2 -g -Wall -Wextra -Wno-unused-parameter -pthread \
             -I$(SPDK_PATH)/include \
             -I$(SPDK_PATH) \
             -I$(SPDK_PATH)/module/sock/ff \
             -I$(FF_PATH)/lib

SPDK_LIB  := $(SPDK_PATH)/build/lib

# ffsock + the SPDK framework libs it and the harness use. --start-group
# resolves the circular refs among them.
SPDK_LIBS := -Wl,--start-group \
             -lspdk_sock_ff -lspdk_sock -lspdk_thread \
             -lspdk_util -lspdk_json -lspdk_log -lspdk_trace \
             -lspdk_env_dpdk \
             -Wl,--end-group

# F-Stack, then SPDK's DPDK (static, includes net_tap + gso + rte_timer + all
# rte_*). pkg-config --static gives the exact rte_* link line and driver
# whole-archive flags.
FF_LIBS    := -L$(FF_PATH)/lib -lfstack
DPDK_LIBS  := $(shell PKG_CONFIG_PATH=$(DPDK_PC) pkg-config --static --libs libdpdk)

# -lz: F-Stack's extra dep beyond SPDK's own link set (see spdk.common.mk note).
SYS_LIBS  := -lpthread -lrt -ldl -lnuma -lm -lssl -lcrypto -luuid -lz

all: phase2

phase2: phase2.o
	$(CC) $(CFLAGS) -o $@ phase2.o \
	  -L$(SPDK_LIB) $(SPDK_LIBS) \
	  $(FF_LIBS) $(DPDK_LIBS) $(SYS_LIBS)

phase2.o: phase2.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f phase2.o phase2

.PHONY: all clean

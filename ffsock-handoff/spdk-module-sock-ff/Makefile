#  SPDX-License-Identifier: BSD-3-Clause
#  F-Stack-backed spdk_sock implementation (ffsock).
#

SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

SO_VER := 6
SO_MINOR := 0

LIBNAME = sock_ff
C_SRCS = ff_sock.c

# F-Stack must be built against THIS SPDK's DPDK (single EAL). Point FF_PATH at
# that build; its lib/ holds ff_api.h, ff_epoll.h and libfstack.a.
FF_PATH ?= /root/mayastore/f-stack
CFLAGS += -I$(FF_PATH)/lib
LOCAL_SYS_LIBS += -L$(FF_PATH)/lib -lfstack

SPDK_MAP_FILE = $(SPDK_ROOT_DIR)/mk/spdk_blank.map

include $(SPDK_ROOT_DIR)/mk/spdk.lib.mk

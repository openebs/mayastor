SHELL := /bin/bash
SPDK_BUILD:= $(CURDIR)/spdk-sys/build
RUST_FLAGS:= RUSTFLAGS="-C link-args=-Wl,-rpath=$(SPDK_BUILD)"

help:
	@echo
	@echo "	Available targets:"
	@echo
	@echo "	init   : checkout the required git submodules"
	@echo "	depend : Install and build runtime dependencies (requires sudo)"
	@echo "	build  : Build mayastor"
	@echo

init:
	@ git submodule update --init --recursive

depend:
	@ if [ "$(shell id -u)" != 0 ]; then \
		echo "To install system packages you have to run \"make depend\" with sudo";\
		exit 1;\
	fi
	@ ./spdk-sys/spdk/scripts/pkgdep.sh
	@ apt-get install pkg-config libblkid-dev -y

build:
	@if [[ "$(shell cargo -V)" != *"nightly"* ]]; then \
		echo "Mayastor requires a nightly rust compiler which could not be found";\
		exit 1;\
	fi
	@ ./spdk-sys/build.sh
	$(RUST_FLAGS) cargo build --release --all-targets

test:
	@ ./spdk-sys/build.sh --enable-debug
	@ $(RUST_FLAGS) cargo build --all-targets

clean:
	@ $(shell cd spdk-sys/spdk; make clean)
	@ cargo clean

.PHONY: help init depend clean build

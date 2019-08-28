FROM ubuntu:19.04
ARG TARGET=debug

RUN apt-get update && apt-get -y install --no-install-recommends \
	xfsprogs \
&& rm -rf /var/lib/apt/lists/*

# grpc client for troubleshooting and testing
COPY target/${TARGET}/mayastor-client /usr/local/bin/
# the main proxy binary
COPY target/${TARGET}/mayastor-client /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/mayastor-client", "-v"]

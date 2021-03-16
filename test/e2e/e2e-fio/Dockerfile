# VERSION 0.1
FROM alpine

MAINTAINER mayastor mayastor@mayadata.io

# Install build deps + permanent dep: libaio
RUN apk --no-cache add \
    	make \
	alpine-sdk \
	zlib-dev \
	libaio-dev \
	linux-headers \
	coreutils \
	coreutils \
	libaio && \
    git clone https://github.com/axboe/fio && \
    cd fio && \
    ./configure && \
    make -j`nproc` && \
    make install && \
    cd .. && \
    rm -rf fio && \
    apk --no-cache del \
    	make \
	alpine-sdk \
	zlib-dev \
	libaio-dev \
	linux-headers \
	coreutils 

RUN apk --no-cache add \
    	gcc musl-dev

ADD e2e_fio.c /

RUN gcc -Wall -o /e2e_fio e2e_fio.c

ENTRYPOINT ["/e2e_fio"]

# VERSION 0.1
FROM alpine

MAINTAINER mayastor mayastor@mayadata.io

RUN apk --no-cache add \
    	fio gcc musl-dev

ADD e2e_fio.c /

RUN gcc -Wall -o /e2e_fio e2e_fio.c
RUN apk --no-cache del \
        gcc musl-dev

ENTRYPOINT ["/e2e_fio"]

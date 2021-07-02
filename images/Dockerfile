ARG GO_VER=1.16.5
ARG ALPINE_VER=3.13

FROM alpine:${ALPINE_VER} as bcdb-base
RUN apk add --no-cache tzdata
RUN echo 'hosts: files dns' > /etc/nsswitch.conf
RUN mkdir /etc/bcdb-server
RUN mkdir /var/bcdb-server

FROM golang:${GO_VER}-alpine${ALPINE_VER} as golang
RUN apk add --no-cache \
    bash \
    binutils-gold \
    gcc \
    git \
    make \
    musl-dev
ADD . $GOPATH/src/github.com/IBM-Blockchain/bcdb-server
WORKDIR $GOPATH/src/github.com/IBM-Blockchain/bcdb-server

FROM golang as bcdb
RUN make binary

FROM bcdb-base
VOLUME /etc/bcdb-server
VOLUME /var/bcdb-server
COPY --from=bcdb /go/src/github.com/IBM-Blockchain/bcdb-server/bin/bdb /usr/local/bin
COPY --from=bcdb /go/src/github.com/IBM-Blockchain/bcdb-server/sampleconfig/config.yml /etc/bcdb-server
COPY --from=bcdb /go/src/github.com/IBM-Blockchain/bcdb-server/sampleconfig/1node-shared-config-bootstrap.yml /etc/bcdb-server
EXPOSE 6001
CMD ["bdb", "start", "--configpath", "/etc/bcdb-server/."]

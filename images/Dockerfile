ARG GO_VER=1.16
ARG ALPINE_VER=3.14

FROM alpine:${ALPINE_VER} as orion-base
RUN apk add --no-cache tzdata
RUN echo 'hosts: files dns' > /etc/nsswitch.conf
RUN mkdir -p /etc/orion-server/config
RUN mkdir -p /etc/orion-server/crypto
RUN mkdir -p /var/orion-server/ledger

FROM golang:${GO_VER}-alpine${ALPINE_VER} as golang
RUN apk add --no-cache \
    bash \
    binutils-gold \
    gcc \
    git \
    make \
    musl-dev
ADD . $GOPATH/src/github.com/hyperledger-labs/orion-server
WORKDIR $GOPATH/src/github.com/hyperledger-labs/orion-server

FROM golang as orion-server
RUN make binary

FROM orion-base
VOLUME /etc/orion-server/config
VOLUME /etc/orion-server/crypto
VOLUME /var/orion-server/ledger
COPY --from=orion-server /go/src/github.com/hyperledger-labs/orion-server/bin/bdb /usr/local/bin
COPY --from=orion-server /go/src/github.com/hyperledger-labs/orion-server/deployment/config-docker /etc/orion-server/config
EXPOSE 6001
CMD ["bdb", "start", "--configpath", "/etc/orion-server/config/."]

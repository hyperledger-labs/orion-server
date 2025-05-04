ARG GO_VER=1.19
ARG UBUNTU_VER=20.04

FROM ubuntu:${UBUNTU_VER} as orion-base
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /val/lib/apt/lists/*
RUN echo 'hosts: files dns' > /etc/nsswitch.conf
RUN mkdir -p /etc/orion-server/config
RUN mkdir -p /etc/orion-server/crypto
RUN mkdir -p /var/orion-server/ledger

FROM golang:${GO_VER} as golang
RUN apt-get update && apt-get install -y --no-install-recommends  \
     bash \
     binutils-gold \
     gcc \
     git \
     make \
     musl-dev && rm -rf /val/lib/apt/lists/*
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

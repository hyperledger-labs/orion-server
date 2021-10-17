ARG GO_VER=1.16.5
ARG ALPINE_VER=3.13

FROM alpine:${ALPINE_VER} as orion-base
RUN apk add --no-cache tzdata
RUN echo 'hosts: files dns' > /etc/nsswitch.conf
RUN mkdir /etc/orion-server
RUN mkdir /var/orion-server

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
VOLUME /etc/orion-server
VOLUME /var/orion-server
COPY --from=orion-server /go/src/github.com/hyperledger-labs/orion-server/bin/bdb /usr/local/bin
COPY --from=orion-server /go/src/github.com/hyperledger-labs/orion-server/sampleconfig/ /etc/orion-server
EXPOSE 6001
CMD ["bdb", "start", "--configpath", "/etc/orion-server/config/."]

ARG ALPINE_VERSION=latest

FROM alpine:${ALPINE_VERSION} as alpine

ARG GCC_VERSION
ENV GCC_VERSION=${GCC_VERSION}
ARG BRANCH_OR_TAG=main

FROM alpine

RUN apk add --quiet --no-cache \
            autoconf \
            automake \
            binutils \
            cmake \
            check-dev \
            curl \
            file \
            git \
            gcc \
            g++ \
            libtool \
            make \
            musl-dev \
            jq \
            patch \
            pkgconf \
            zlib-dev

RUN git clone --depth=1 -b $BRANCH_OR_TAG -q https://github.com/google/googletest.git /googletest
RUN mkdir -p /googletest/build
WORKDIR /googletest/build
RUN cmake .. && make && make install
RUN mkdir -p /code
WORKDIR /code
RUN rm -rf /googletest
FROM --platform=linux/amd64 ubuntu:18.04

RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    git \
    autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

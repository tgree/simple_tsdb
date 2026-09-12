# Use Debian Buster to match Raspbian 10
FROM debian:buster-slim

# Prevent interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Point apt to the Debian Archive mirrors since Buster is EOL
RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i '/buster-updates/d' /etc/apt/sources.list

# Install g++ 8.3.0, make, and other essential build utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    git \
    autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /src

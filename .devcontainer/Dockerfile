FROM mcr.microsoft.com/devcontainers/go:1-1.23-bookworm
RUN apt-get update \
    && apt-get install -y sqlite3 gcc-x86-64-linux-gnu gcc-aarch64-linux-gnu \
    && apt-get clean
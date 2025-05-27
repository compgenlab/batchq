FROM mcr.microsoft.com/devcontainers/go:1-1.23-bookworm
RUN apt-get update \
    && apt-get install -y sqlite3 gcc-x86-64-linux-gnu gcc-aarch64-linux-gnu \
    && apt-get clean \
    && mkdir -p /opt/musl-cross \
    && wget https://musl.cc/x86_64-linux-musl-cross.tgz \
    && wget https://musl.cc/aarch64-linux-musl-cross.tgz \
    && tar -xzf x86_64-linux-musl-cross.tgz -C /opt/musl-cross \
    && tar -xzf aarch64-linux-musl-cross.tgz -C /opt/musl-cross
ENV PATH="/opt/musl-cross/x86_64-linux-musl-cross/bin:/opt/musl-cross/aarch64-linux-musl-cross/bin:${PATH}"
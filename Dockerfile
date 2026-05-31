FROM debian:bookworm-slim

LABEL org.opencontainers.image.source="https://github.com/wpelevator/ngx_cache_pilot"
LABEL org.opencontainers.image.description="Debian development environment for ngx_cache_pilot"

ENV DEBIAN_FRONTEND=noninteractive
ENV NGINX_BUILD_PREFIX=/opt/nginx
ENV PATH=${NGINX_BUILD_PREFIX}/sbin:${PATH}

# Perl packages below are dependencies for openresty/test-nginx.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        astyle \
        bash \
        build-essential \
        ca-certificates \
        cpanminus \
        curl \
        dos2unix \
        git \
        libpcre3-dev \
        libssl-dev \
        libipc-run-perl \
        liblist-moreutils-perl \
        libtest-base-perl \
        libtest-longstring-perl \
        libtext-diff-perl \
        liburi-perl \
        libwww-perl \
        perl \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth=1 --branch v0.32 https://github.com/openresty/test-nginx.git /opt/test-nginx \
    && cpanm --notest /opt/test-nginx

ARG NGINX_VERSION=1.25.5
ENV NGINX_VERSION=${NGINX_VERSION}
ENV NGINX_SRC_DIR=/opt/nginx-src/nginx-${NGINX_VERSION}

RUN mkdir -p /opt/nginx-src \
    && curl -fsSLo /tmp/nginx.tar.gz "https://nginx.org/download/nginx-${NGINX_VERSION}.tar.gz" \
    && tar -xzf /tmp/nginx.tar.gz -C /opt/nginx-src \
    && rm /tmp/nginx.tar.gz

RUN cd "${NGINX_SRC_DIR}" \
    && ./configure \
        --prefix="${NGINX_BUILD_PREFIX}" \
        --with-http_ssl_module \
        --with-http_stub_status_module \
        --with-http_realip_module \
        --with-threads \
    && make -j"$(nproc)" \
    && make install

WORKDIR /workspace

CMD ["/bin/bash"]

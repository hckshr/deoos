FROM node:22.23.2-bookworm-slim AS node
FROM python:3.12-slim-bookworm

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# Keep this pin identical to deoos.lock.
ARG DEOOS_VERSION=0.8.0
ARG TARGETARCH
# Set to a filename in docker/offline-tarballs/ to build without downloading deoos.
# Leave it empty and the build uses that directory's matching archive when present.
ARG DEOOS_TARBALL=

COPY docker/offline-tarballs/ /tmp/offline-tarballs/

RUN set -eu; \
    arch=${TARGETARCH:-$(dpkg --print-architecture)}; \
    case "$arch" in amd64) target=x64 ;; arm64) target=arm64 ;; *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; esac; \
    archive="deoos-${DEOOS_VERSION}-linux-${target}.tar.gz"; \
    mkdir -p /tmp/release /opt/deoos /app; \
    if [ -n "$DEOOS_TARBALL" ]; then \
      cp "/tmp/offline-tarballs/$DEOOS_TARBALL" "/tmp/release/$archive"; \
      cp /tmp/offline-tarballs/SHA256SUMS /tmp/release/SHA256SUMS; \
    elif [ -f "/tmp/offline-tarballs/$archive" ]; then \
      cp "/tmp/offline-tarballs/$archive" "/tmp/release/$archive"; \
      cp /tmp/offline-tarballs/SHA256SUMS /tmp/release/SHA256SUMS; \
    else \
      base="https://github.com/hckshr/deoos/releases/download/${DEOOS_VERSION}"; \
      curl -fL --retry 3 "${base}/${archive}" -o "/tmp/release/$archive"; \
      curl -fsSL --retry 3 "${base}/SHA256SUMS" -o /tmp/release/SHA256SUMS; \
    fi; \
    cd /tmp/release; \
    grep "  ${archive}$" SHA256SUMS > selected.sha256; \
    test -s selected.sha256; \
    sha256sum -c selected.sha256; \
    tar -xzf "$archive" --strip-components=1 -C /opt/deoos; \
    python -m pip install --no-cache-dir /opt/deoos/python/*.whl duckdb==1.5.6 boto3==1.42.97 'psycopg[binary]==3.3.8'; \
    cd /app; \
    npm install --no-audit --no-fund /opt/deoos/node/*.tgz @duckdb/node-api@1.5.6-r.1 @aws-sdk/client-s3@3.1158.0; \
    rm -rf /tmp/release

COPY python /app/python
COPY typescript /app/typescript
COPY tests /app/tests
COPY docker/entrypoint.sh /usr/local/bin/deoos-demo
RUN chmod +x /usr/local/bin/deoos-demo
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/tests \
    ENGINE_BINARY=/opt/deoos/bin/deoos-server \
    DEOOS_NODE_SDK=/app/node_modules/deoos/dist/index.js \
    DEOOS_NODE_PACKAGE=/app/node_modules/deoos
ENTRYPOINT ["deoos-demo"]

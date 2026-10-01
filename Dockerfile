FROM alpine:3.24

# GoReleaser hands buildx a context holding one directory per target platform,
# so the binary is at linux/amd64/ptah or linux/arm64/ptah rather than at the
# root. TARGETPLATFORM is set by buildx for the platform being built.
ARG TARGETPLATFORM

RUN apk add --no-cache ca-certificates

COPY LICENSE /usr/share/licenses/ptah/LICENSE

COPY $TARGETPLATFORM/ptah /usr/local/bin/ptah
COPY $TARGETPLATFORM/ptah-compat /usr/local/bin/ptah-compat
COPY $TARGETPLATFORM/ptah-ls /usr/local/bin/ptah-ls

ENTRYPOINT ["/usr/local/bin/ptah"]

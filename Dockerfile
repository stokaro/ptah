FROM alpine:3.24

# GoReleaser hands buildx a context holding one directory per target platform,
# so the binary is at linux/amd64/ptah or linux/arm64/ptah rather than at the
# root. TARGETPLATFORM is set by buildx for the platform being built.
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache ca-certificates

COPY LICENSE /usr/share/licenses/ptah/LICENSE

COPY $TARGETPLATFORM/ptah /usr/local/bin/ptah
COPY $TARGETPLATFORM/ptah-compat /usr/local/bin/ptah-compat
COPY $TARGETPLATFORM/ptah-ls /usr/local/bin/ptah-ls

# The pinned YDB provider, at the bundled location: a providers directory in the
# directory that holds the resolved ptah executable. The release build stages it
# from the pin (internal/cmd/providerbundle) and verifies its digests, so the
# image needs no network and no `ptah providers install` at run time.
COPY .release-providers/${TARGETOS}_${TARGETARCH}/providers /usr/local/bin/providers

ENTRYPOINT ["/usr/local/bin/ptah"]

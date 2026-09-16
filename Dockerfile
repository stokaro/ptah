FROM alpine:3.24

# GoReleaser hands buildx a context holding one directory per target platform,
# so the binary is at linux/amd64/ptah or linux/arm64/ptah rather than at the
# root. TARGETPLATFORM is set by buildx for the platform being built.
ARG TARGETPLATFORM

RUN apk add --no-cache ca-certificates

COPY $TARGETPLATFORM/ptah /usr/local/bin/ptah

ENTRYPOINT ["/usr/local/bin/ptah"]

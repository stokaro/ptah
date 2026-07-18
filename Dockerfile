FROM alpine:3.24

RUN apk add --no-cache ca-certificates

COPY ptah /usr/local/bin/ptah

ENTRYPOINT ["/usr/local/bin/ptah"]

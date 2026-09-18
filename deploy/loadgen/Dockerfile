# Binary-wrap: cross-compile sandbox-loadgen on the build host, then COPY into
# distroless. Build context must contain the prebuilt linux/amd64 binary.
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" \
#     -o sandbox-loadgen ./cmd/sandbox-loadgen/
#   docker build -f deploy/loadgen/Dockerfile -t <cr>/g0031/sandbox-loadgen:<tag> .
FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
COPY sandbox-loadgen /usr/local/bin/sandbox-loadgen
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sandbox-loadgen"]

FROM --platform=$BUILDPLATFORM golang:1.27.1@sha256:3680233e3204827fbdc66088528ae6d4b3d034f51d03a99d454f6de034888244 AS build
ARG TARGETOS
ARG TARGETARCH
ARG BINARY=sandbox-apiserver
ARG VERSION=dev
ARG REVISION=unknown
ARG BUILTAT=unknown
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath \
      -ldflags="-s -w \
        -X github.com/cocoonstack/sandbox-operator/version.VERSION=${VERSION} \
        -X github.com/cocoonstack/sandbox-operator/version.REVISION=${REVISION} \
        -X github.com/cocoonstack/sandbox-operator/version.BUILTAT=${BUILTAT}" \
      -o /app ./cmd/${BINARY}

FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3
COPY --from=build /app /app

ENTRYPOINT ["/app"]

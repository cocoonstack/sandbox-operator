FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:e519d3b0a5b285e5aa53ab9eef02bc6bbb9cb04608389b743a136a9ce75f0d1e AS build
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

FROM gcr.io/distroless/static-debian13:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
COPY --from=build /app /app

ENTRYPOINT ["/app"]

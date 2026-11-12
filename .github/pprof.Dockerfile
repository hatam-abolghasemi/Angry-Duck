# Debug image with Go's /debug/pprof/ built in, for the "pprof images"
# workflow only. The workflow passes the build and runtime images from the
# tag's own app/Dockerfile.<component>, and GO_TAGS = that Dockerfile's tags
# plus "pprof", so the binary matches the release one apart from profiling.
ARG GO_IMAGE
ARG RUNTIME_IMAGE

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS TARGETARCH COMPONENT GO_TAGS
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd/${COMPONENT} ./cmd/${COMPONENT}
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -tags "$GO_TAGS" -trimpath -ldflags="-s -w" -o /out/angryduck ./cmd/${COMPONENT}

FROM ${RUNTIME_IMAGE}
COPY --from=build /out/angryduck /angryduck
ENTRYPOINT ["/angryduck"]

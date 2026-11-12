FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY api ./api
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/pulsestream ./cmd/pulsestream && CGO_ENABLED=0 go build -trimpath -o /out/event-generator ./cmd/event-generator && CGO_ENABLED=0 go build -trimpath -o /out/replay-cli ./cmd/replay-cli
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/ /
USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/pulsestream"]

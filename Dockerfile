FROM golang:1.25 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/ch-sync .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY --from=builder /out/ch-sync /usr/local/bin/ch-sync
COPY docker/config.container.yaml /workspace/config.yaml
ENTRYPOINT ["ch-sync"]
CMD ["--help"]

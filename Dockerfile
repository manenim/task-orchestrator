# Keep the Go series aligned with go.mod; pass GO_VERSION to select a patch.
ARG GO_VERSION=1.24
FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate && \
    CGO_ENABLED=0 go build -trimpath -o /out/smoke ./cmd/smoke

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/ /app/
COPY internal/adapter/postgres/schema.sql /app/internal/adapter/postgres/schema.sql
EXPOSE 50051
ENTRYPOINT ["/app/server"]

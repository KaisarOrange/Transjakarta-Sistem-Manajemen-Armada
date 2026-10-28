# One image, three binaries (/api, /worker, /publisher); docker-compose picks one per service.
ARG GO_VERSION=1.27
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
# Dependencies first, so editing code does not re-download modules.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/api /out/worker /out/publisher /
USER app
EXPOSE 8080
CMD ["/api"]

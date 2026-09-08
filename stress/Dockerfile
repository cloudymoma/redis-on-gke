FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /redis-stress ./cmd/redis-stress

# alpine (not scratch) on purpose: the Job wrapper needs sh + sleep, and
# kubectl cp needs tar in the container.
FROM alpine:3.21
RUN adduser -D -u 1000 stress
COPY --from=build /redis-stress /redis-stress
USER 1000
ENTRYPOINT ["/redis-stress"]

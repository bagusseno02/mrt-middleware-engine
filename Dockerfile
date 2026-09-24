FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pmcollector ./cmd/pmcollector

FROM build AS test
RUN apk add --no-cache build-base
ENV CGO_ENABLED=1
CMD ["go", "test", "-race", "./..."]

FROM alpine:3.23
RUN apk add --no-cache ca-certificates && addgroup -g 10001 collector && adduser -D -u 10001 -G collector collector \
    && mkdir -p /app/data && chown -R collector:collector /app
WORKDIR /app
COPY --from=build /out/pmcollector /usr/local/bin/pmcollector
COPY config.yaml /app/config.yaml
USER 10001:10001
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["pmcollector"]
CMD ["serve", "-config", "/app/config.yaml"]

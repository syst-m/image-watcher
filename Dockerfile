# Build stage
FROM golang:1.24-alpine AS builder

RUN apk --no-cache add git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o image-watcher .

# Final stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates kubectl && \
    addgroup -S appgroup && \
    adduser -S appuser -G appgroup

COPY --from=builder /src/image-watcher /usr/local/bin/image-watcher

USER appuser
ENTRYPOINT ["image-watcher"]

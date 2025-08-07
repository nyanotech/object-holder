FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o object-holder .

FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/object-holder .

ENTRYPOINT ["./object-holder"]

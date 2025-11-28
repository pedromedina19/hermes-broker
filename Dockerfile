FROM golang:1.25.4 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o hermes cmd/server/main.go

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/hermes .

EXPOSE 50051
CMD ["./hermes"]
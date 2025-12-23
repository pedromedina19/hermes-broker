FROM golang:1.25.4 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Compila o binário estático
RUN CGO_ENABLED=0 GOOS=linux go build -o hermes cmd/server/main.go

FROM alpine:latest

# Instala certificados de segurança (Boa prática)
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/hermes .

# Porta gRPC
EXPOSE 50051
# Porta HTTP/GraphQL
EXPOSE 8080

CMD ["./hermes"]
FROM golang:1.25.4 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o hermes cmd/server/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates curl 
# (Adicionei o curl no alpine para debug se precisar entrar no container)

WORKDIR /root/
COPY --from=builder /app/hermes .

# Porta gRPC (Clientes)
EXPOSE 50051
# Porta HTTP/GraphQL (Clientes)
EXPOSE 8080
# Porta RAFT (Interna entre nós do cluster)
EXPOSE 6000 

CMD ["./hermes"]
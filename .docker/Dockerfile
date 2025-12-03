FROM golang:1.25.3-alpine AS builder

WORKDIR /app

COPY . .

RUN go build -o app .

FROM golang:1.25.3-alpine

# Get CAs
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=builder /app/app .

CMD ["./app"]
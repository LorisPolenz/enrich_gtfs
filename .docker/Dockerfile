FROM golang:1.25.3 AS builder

WORKDIR /app

COPY . .

RUN go build -o app .

FROM gcr.io/distroless/static:nonroot

WORKDIR /app

COPY --from=builder /app/app .

ENTRYPOINT ["/app/app"]
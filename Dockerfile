FROM golang:1.21 as build-stage

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o /http-to-kafka

FROM debian:12 AS release-stage

WORKDIR /

COPY --from=build-stage /http-to-kafka /http-to-kafka

EXPOSE 8080

CMD ["/http-to-kafka"]

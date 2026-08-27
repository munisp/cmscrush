FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/decision-service ./cmd/decision-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/decision-service /decision-service
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/decision-service"]

# Builds the server and the fake model into one small image. Compose picks
# which program each container runs.

FROM golang:1.27 AS build
WORKDIR /src

# Download dependencies first, so Docker can cache them between code changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /
USER nonroot
ENTRYPOINT ["/server"]

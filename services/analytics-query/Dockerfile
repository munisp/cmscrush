FROM rust:1.98-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 crush
COPY --from=build /src/target/release/crush-analytics-query /usr/local/bin/analytics-query
USER 10001
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/analytics-query"]

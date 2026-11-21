FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release && strip target/release/pipeline target/release/micro
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
COPY --from=build /src/target/release/micro /app/micro
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/bench.sh /app/bench.sh
USER 1000:1000
CMD ["/app/bench.sh"]

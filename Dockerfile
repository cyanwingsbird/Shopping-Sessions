FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin pipeline && strip target/release/pipeline
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/ab10.sh /app/ab10.sh
USER 1000:1000
CMD ["/bin/sh", "/app/ab10.sh"]

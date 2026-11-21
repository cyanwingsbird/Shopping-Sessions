# Shopping Sessions: single static-ish Rust binary, no runtime engine to start.
# Two builds of the same source: x86-64-v3 (AVX2) and x86-64-v4 (AVX-512); the v3 binary
# re-executes the v4 one at startup when the CPU supports it.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin pipeline && strip target/release/pipeline
RUN RUSTFLAGS="-C target-cpu=x86-64-v4" cargo build --release --bin pipeline --target-dir /v4 && strip /v4/release/pipeline
RUN cargo build --release --bin micro && strip target/release/micro
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
COPY --from=build /v4/release/pipeline /app/pipeline-v4
COPY --from=build /src/target/release/micro /app/micro
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/bench.sh /app/bench.sh
USER 1000:1000
CMD ["/app/bench.sh"]

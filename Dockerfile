# Shopping Sessions: single static-ish Rust binary, no runtime engine to start.
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release && strip target/release/pipeline

FROM debian:bookworm-slim
COPY --from=build /src/target/release/pipeline /app/pipeline
USER 1000:1000
CMD ["/app/pipeline"]

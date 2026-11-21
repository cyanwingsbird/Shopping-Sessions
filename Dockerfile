# Shopping Sessions: one static Rust binary in an empty image (nothing else to set up at container start).
FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static" cargo build --release --bin pipeline --target x86_64-unknown-linux-gnu \
 && strip target/x86_64-unknown-linux-gnu/release/pipeline

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-gnu/release/pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]

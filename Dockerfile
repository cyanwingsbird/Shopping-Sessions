FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin va && strip target/release/va && cp target/release/va /va \
 && RUSTFLAGS="-C target-cpu=x86-64-v4" cargo build --release --bin va --target-dir /t4 && strip /t4/release/va && cp /t4/release/va /vb \
 && RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release --bin va --target-dir /t5 && strip /t5/release/va && cp /t5/release/va /vc
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /va /vb /vc /app/
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/ab3.sh /app/ab3.sh
USER 1000:1000
ENV BENCH_EVENTS=20000000 REPS=3 COMPACT=1 VARIANTS="a:va b:vb c:vc d:va e:vb f:vc"
CMD ["/bin/sh", "/app/ab3.sh"]

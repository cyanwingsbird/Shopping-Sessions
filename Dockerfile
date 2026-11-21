FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin va && strip target/release/va
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/va /app/
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/ab3.sh /app/ab3.sh
USER 1000:1000
ENV BENCH_EVENTS=20000000 REPS=3 VARIANTS="a:va:SS_PINMODE=1 b:va:SS_PINMODE=2 c:va:SS_PINMODE=0 d:va:SS_PINMODE=1 e:va:SS_PINMODE=2 f:va:SS_PINMODE=0 g:va:SS_PINMODE=2:SS_THREADS=16 h:va:SS_PINMODE=0:SS_THREADS=16"
CMD ["/bin/sh", "/app/ab3.sh"]

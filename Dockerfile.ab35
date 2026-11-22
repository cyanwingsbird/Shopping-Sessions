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
ENV BENCH_EVENTS=60000000 REPS=2 VARIANTS="a:va b:va:SS_SUB3MIN=100000000 c:va:SS_SUB3=8 d:va:SS_SUB3=2 e:va:SS_Q=1536 f:va:SS_Q=1536:SS_SUB3=2 g:va:SS_Q=2048 h:va"
CMD ["/bin/sh", "/app/ab3.sh"]

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
ENV BENCH_EVENTS=20000000 REPS=2 COMPACT=1 VARIANTS="a:va b:va:SS_ABL=3 c:va:SS_ABL=2 d:va:SS_ABL=1 e:va:SS_ABL=0 f:va:SS_ABL=3:SS_THREADS=4 g:va:SS_THREADS=4 h:va:SS_ABL=1:SS_THREADS=4"
CMD ["/bin/sh", "/app/ab3.sh"]

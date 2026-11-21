FROM rust:1.91-slim-bookworm AS b91
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin pipeline && strip target/release/pipeline

FROM rust:1.97-slim-bookworm AS b97
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
COPY pgo.profdata /pgo.profdata
RUN cargo build --release --bin pipeline && strip target/release/pipeline && cp target/release/pipeline /vb \
 && RUSTFLAGS="-C target-cpu=x86-64-v3 -C profile-use=/pgo.profdata" cargo build --release --bin pipeline --target-dir /t2 \
 && strip /t2/release/pipeline && cp /t2/release/pipeline /vc
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=b91 /src/target/release/pipeline /app/va
COPY --from=b97 /vb /vc /app/
COPY --from=b97 /gen/target/release/generate /app/generate
COPY bench/ab3.sh /app/ab3.sh
USER 1000:1000
ENV BENCH_EVENTS=20000000 REPS=3 VARIANTS="a:va b:vb c:vc d:va e:vb f:vc"
CMD ["/bin/sh", "/app/ab3.sh"]

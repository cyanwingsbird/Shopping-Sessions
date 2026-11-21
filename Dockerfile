FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin va --bin vb && strip target/release/va target/release/vb
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/va /src/target/release/vb /app/
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/ab3.sh /app/ab3.sh
USER 1000:1000
ENV BENCH_EVENTS=20000000 REPS=4 VARIANTS="a:va b:vb c:vb:SS_LINKP=0 d:va e:vb f:vb:SS_LINKP=0"
CMD ["/bin/sh", "/app/ab3.sh"]

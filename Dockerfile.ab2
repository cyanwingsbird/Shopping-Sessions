FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release && cd target/release && strip va vb vc vd ve
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/va /src/target/release/vb /src/target/release/vc /src/target/release/vd /src/target/release/ve /app/
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/ab2.sh /app/ab2.sh
USER 1000:1000
ENV VARIANTS="a:va b:vb c:vc d:vd e:ve f:va:SS_P1=512 g:va:SS_Q=512 h:va:SS_Q=2048"
CMD ["/bin/sh", "/app/ab2.sh"]

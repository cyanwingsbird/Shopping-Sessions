FROM rust:1.91-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY .cargo .cargo
COPY src src
RUN cargo build --release --bin va --bin ve && strip target/release/va target/release/ve
RUN RUSTFLAGS="-C target-cpu=skylake" CARGO_TARGET_DIR=/tmp/tsk cargo build --release --bin vb && strip /tmp/tsk/release/vb && cp /tmp/tsk/release/vb target/release/vb
RUN RUSTFLAGS="-C target-cpu=cascadelake" CARGO_TARGET_DIR=/tmp/tcl cargo build --release --bin vc --bin vd && strip /tmp/tcl/release/vc /tmp/tcl/release/vd && cp /tmp/tcl/release/vc /tmp/tcl/release/vd target/release/
COPY bench/generator /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/va /src/target/release/vb /src/target/release/vc /src/target/release/vd /src/target/release/ve /app/
COPY --from=build /gen/target/release/generate /app/generate
COPY bench/ab2.sh /app/ab2.sh
USER 1000:1000
ENV VARIANTS="a:va b:vb c:vc d:vd e:ve f:va g:vb h:vc"
CMD ["/bin/sh", "/app/ab2.sh"]

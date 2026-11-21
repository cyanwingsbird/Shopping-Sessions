# Shopping Sessions: Rust pipeline

A single dependency-free Rust binary. The Dockerfile at the root builds it and runs it as the image's `CMD`.

How it works:
1. All CSVs are memory-mapped into one address space. The files are cut into ~1 MB chunks that 8 threads parse with AVX2 masks: lines, commas and quotes are checked exactly against `rules.md`.
2. Events are scattered by an `event_id` hash. Each partition dedups on the full `event_id` (smallest `ingest_ts`, before validation), and the rows are then mapped to their person through a union-find over `links/`.
3. Records are scattered by person. Each person's events and clicks are sorted, then sessionized, credited and assisted with a sliding 7-day window.
4. Big buffers come from a small allocator that uses transparent huge pages. Records carry a 64-bit `event_id` hash and the parsed `ingest_ts`, so dedup never goes back to the CSV.
5. Parquet is written by a small hand-written writer (plain encoding, uncompressed, `day` as `DATE`).

Local check: `python3 scripts/run_fixtures.py fixtures -- target/release/pipeline` and `scripts/compare.py`.
`ref/reference.py` is a slow, literal Python implementation used to cross-check other seeds.

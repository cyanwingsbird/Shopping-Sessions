#!/bin/sh
# Full local check of target/release/pipeline: fixtures (3 modes), dev answer, p20 vs /tmp/o20a, seeds vs refs.
B=${1:-/home/claude/ss/target/release/pipeline}
cd /home/claude/ss
python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
SS_STRICT=1 python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
SS_CLICK_DEDUP=1 python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
rm -rf /tmp/vdev; $B /home/claude/data/dev /tmp/vdev; echo "dev $(python3 scripts/compare.py /home/claude/dl/x-dev-answer/dev-answer /tmp/vdev 2>&1 | tail -1)"
for t in 1 2 5 8; do rm -rf /tmp/v20; SS_THREADS=$t $B /home/claude/data/p20 /tmp/v20; echo "p20 t$t $(python3 scripts/compare.py /tmp/o20a /tmp/v20 2>&1 | tail -1)"; done
for s in 101 202 303 404 505; do for t in 3 8; do rm -rf /tmp/vs; SS_THREADS=$t $B /tmp/seeds/s$s /tmp/vs; echo "s$s t$t $(python3 scripts/compare.py /tmp/seeds/ref$s /tmp/vs 2>&1 | tail -1)"; done; done

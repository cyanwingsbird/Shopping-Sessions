#!/bin/sh
# Check on the organizers' new generator (v5 IDs): fixtures (3 modes), new dev answer, p20v5 and seeds vs v30i outputs.
B=${1:-/home/claude/ss/target/release/pipeline}
cd /home/claude/ss
python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
SS_STRICT=1 python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
SS_CLICK_DEDUP=1 python3 scripts/run_fixtures.py fixtures -- $B 2>&1 | tail -1
rm -rf /tmp/vdev; $B /home/claude/data/dev5 /tmp/vdev; echo "dev5 $(python3 scripts/compare.py /home/claude/dl5/xa/dev-answer /tmp/vdev 2>&1 | tail -1)"
rm -rf /tmp/vdev; SS_STRICT=1 $B /home/claude/data/dev5 /tmp/vdev; echo "dev5 strict $(python3 scripts/compare.py /home/claude/dl5/xa/dev-answer /tmp/vdev 2>&1 | tail -1)"
rm -rf /tmp/vdev; $B /home/claude/data/dev /tmp/vdev; echo "dev $(python3 scripts/compare.py /home/claude/dl/x-dev-answer/dev-answer /tmp/vdev 2>&1 | tail -1)"
for t in 1 2 8; do rm -rf /tmp/v20; SS_THREADS=$t $B /home/claude/data/p20v5 /tmp/v20; echo "p20v5 t$t $(python3 scripts/compare.py /tmp/o20v5 /tmp/v20 2>&1 | tail -1)"; done
for s in 11 22 33; do for t in 3 8; do rm -rf /tmp/vs; SS_THREADS=$t $B /tmp/seeds5/s$s /tmp/vs; echo "s$s t$t $(python3 scripts/compare.py /tmp/seeds5/ref$s /tmp/vs 2>&1 | tail -1)"; done; done

#!/bin/sh
# On-scorer A/B: generate a 20M-event set in /scratch, run each variant binary 3 times (interleaved),
# then write correct tables for /data plus one extra user_daily row per variant carrying its timings
# (user_id: walls; top_category: stage1/stage2/stage3 ms and user CPU per run).
S=${SCRATCH_DIR:-/scratch}
V=${VARIANTS:-"va vb vc vd ve"}
rm -f "$S/runs.txt"
/app/generate --seed 7 --events ${BENCH_EVENTS:-20000000} --clicks $(( ${BENCH_EVENTS:-20000000} / 4 )) --output "$S/gen" >/dev/null 2>&1
cat "$S"/gen/*/*.csv > /dev/null
one() {
  rm -rf "$S/out"; mkdir -p "$S/out"
  a=$(date +%s%N)
  r=$(env SS_TIMING=1 "$2" "$S/gen" "$S/out" 2>&1 | awk '
    function ms(x) { if (x ~ /ms$/) { sub(/ms$/, "", x); return x + 0 } if (x ~ /µs$/) return 0; sub(/s$/, "", x); return x * 1000 }
    /^stage1/ { s1 = ms($3) } /^stage2/ { s2 = ms($3) } /^stage3/ { s3 = ms($3) } /^total/ { us = $4 }
    /^phases/ { for (i = 3; i <= NF; i++) { split($i, kv, "="); ph = ph substr(kv[1], 1, 2) kv[2] "," } }
    END { printf "%.0f/%.0f/%.0f/%s/%s", s1, s2 - s1, s3 - s2, us, ph }')
  b=$(date +%s%N)
  echo "$1 $(( (b - a) / 1000000 )) $r" >> "$S/runs.txt"
}
for i in 1 2 3; do for v in $V; do one $v /app/$v; done; done
cat "$S/runs.txt" >&2
set -- $V
/app/$1 /data /output
for v in $V; do
  printf '!%s w %s\t0\t0\t0\t0\t%s\n' "$v" "$(grep "^$v " "$S/runs.txt" | awk '{printf "%s ", $2}')" "$(grep "^$v " "$S/runs.txt" | awk '{printf "%s ", $3}')"
done > "$S/rows.txt"
cat "$S/rows.txt" >&2
SS_EMIT_TEXT="$S/rows.txt" /app/$1 x /output/user_daily/zz-report.parquet
exit 0

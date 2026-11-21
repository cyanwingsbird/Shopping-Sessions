#!/bin/sh
# On-scorer A/B: generate a set in /scratch, run each variant REPS times (interleaved), then write the
# correct tables for /data (with the first variant's binary) plus one user_daily row per variant pair.
# A variant is label:binary[:ENV=value[:ENV=value]...]
S=${SCRATCH_DIR:-/scratch}
E=${BENCH_EVENTS:-20000000}
V=${VARIANTS:-"a:va b:vb c:vc d:vd e:ve"}
/app/generate --seed 7 --events $E --clicks $(( E / 4 )) --output "$S/gen" >/dev/null 2>&1
cat "$S"/gen/*/*.csv > /dev/null
rm -f "$S/runs.txt"
one() {
  spec=$1
  label=${spec%%:*}; rest=${spec#*:}; bin=${rest%%:*}
  envs=""; [ "$rest" != "$bin" ] && envs=$(echo "${rest#*:}" | tr ':' ' ')
  rm -rf "$S/out"; mkdir -p "$S/out"
  a=$(date +%s%N)
  r=$(env SS_TIMING=1 $envs /app/$bin "$S/gen" "$S/out" 2>&1 | awk '
    function ms(x) { if (x ~ /ms$/) { sub(/ms$/, "", x); return x + 0 } if (x ~ /µs$/) return 0; sub(/s$/, "", x); return x * 1000 }
    /^stage1/ { s1 = ms($3) } /^stage2/ { s2 = ms($3) } /^stage3/ { s3 = ms($3) } /^total/ { tt = ms($2); us = $4; sy = $6 }
    /^links done/ { li = ms($3) }
    /^phases/ { for (i = 3; i <= NF; i++) { split($i, kv, "="); if (kv[2] != "" && kv[2] != "0") ph = ph substr(kv[1], 1, 2) kv[2] "," } }
    END { printf "%.0f/%.0f/%.0f t%.0f l%.0f u%s s%s %s", s1, s2 - s1, s3 - s2, tt, li, us, sy, ph }')
  b=$(date +%s%N)
  echo "$label w$(( (b - a) / 1000000 )) $r" >> "$S/runs.txt"
}
for i in $(seq ${REPS:-3}); do for v in $V; do one $v; done; done
cat "$S/runs.txt" >&2
first=${V%% *}; fb=${first#*:}; fb=${fb%%:*}
/app/$fb /data /output
# first run in full (with phases), the others as wall, stage times and internal total
row() { grep "^$1 " "$S/runs.txt" | awk '{ if (NR == 1) { $1 = ""; printf "%s;", $0 } else printf "%s %s %s;", $2, $3, $4 }'; }
set -- $V
n=0
while [ $# -gt 0 ]; do
  l1=${1%%:*}; shift
  if [ $# -gt 0 ]; then l2=${1%%:*}; shift; else l2=""; fi
  n=$((n + 1))
  printf '!%s %s %s\t0\t0\t0\t0\t%s %s\n' "$n" "$l1" "$(row $l1)" "$l2" "$( [ -n "$l2" ] && row $l2)"
done > "$S/rows.txt"
cat "$S/rows.txt" >&2
SS_EMIT_TEXT="$S/rows.txt" /app/$fb x /output/user_daily/zz-report.parquet
exit 0

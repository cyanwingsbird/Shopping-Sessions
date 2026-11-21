#!/bin/sh
# Diagnostic run: generate a 20M-event set in /scratch, time the pipeline under several settings,
# print everything to stderr, and write no output so the dev check fails and shows these logs.
S=${SCRATCH_DIR:-/scratch}
log() { echo "$@" >&2; echo "$@" >> "$S/report.txt"; }
rm -f "$S/report.txt" "$S/runs.txt"
log "== env: nproc $(nproc) | $(grep -m1 'model name' /proc/cpuinfo) | kernel $(uname -r)"
log "== thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>&1) | defrag: $(cat /sys/kernel/mm/transparent_hugepage/defrag 2>&1)"
log "== mem: $(grep -E 'MemTotal|MemAvailable' /proc/meminfo | tr -s ' ' | tr '\n' ' ') | flags avx512bw: $(grep -c avx512bw /proc/cpuinfo)"
t0=$(date +%s%N)
/app/generate --seed 7 --events ${BENCH_EVENTS:-20000000} --clicks $(( ${BENCH_EVENTS:-20000000} / 4 )) --output "$S/gen" >/dev/null 2>&1
t1=$(date +%s%N)
log "== generate 20M: $(( (t1 - t0) / 1000000 )) ms, $(du -sh "$S/gen" | cut -f1)"
cat "$S"/gen/*/*.csv > /dev/null
run() {
  name=$1; shift
  rm -rf "$S/out"; mkdir -p "$S/out"
  a=$(date +%s%N)
  r=$(env SS_TIMING=1 "$@" /app/pipeline "$S/gen" "$S/out" 2>&1 | awk '
    function ms(x) { if (x ~ /ms$/) { sub(/ms$/, "", x); return x + 0 } if (x ~ /µs$/) return 0; sub(/s$/, "", x); return x * 1000 }
    /^setup/ { su = ms($2) } /^links done/ { li = ms($3) } /^stage1/ { s1 = ms($3) } /^stage2/ { s2 = ms($3) } /^stage3/ { s3 = ms($3) }
    /^total/ { to = ms($2); us = $4; sy = $6; mf = $8; rss = $10 }
    END { printf "su%.0f li%.0f s1:%.0f s2:%.0f s3:%.0f tot%.0f u%s s%s f%s m%s", su, li, s1, s2, s3, to, us, sy, mf, rss }')
  b=$(date +%s%N)
  log "$name w$(( (b - a) / 1000000 )) $r"
  echo "$name $(( (b - a) / 1000000 )) $r" >> "$S/runs.txt"
}
/app/micro "$S/gen" > "$S/micro.txt" 2>&1
cat "$S/micro.txt" >&2
run v4a
run v3a SS_NO_V4=1
run v4b
run v3b SS_NO_V4=1
run v4t1 SS_THREADS=1
run v3t1 SS_THREADS=1 SS_NO_V4=1
run v4c
run v3c SS_NO_V4=1
# correct tables for /data, plus 5 extra user_daily rows (user_id starts with "!") carrying text
/app/pipeline /data /output
{
  printf '!0a %s\t0\t0\t0\t0\t%s\n' "$(grep '^info' "$S/micro.txt" | cut -c6-) $(grep '^mount' "$S/micro.txt" | tr '\n' ' ')" "$(grep '^res' "$S/micro.txt" | cut -c5-)"
  printf '!1 %s\t0\t0\t0\t0\t%s\n' "$(grep '^v4a ' "$S/runs.txt")" "$(grep '^v3a ' "$S/runs.txt")"
  printf '!2 %s\t0\t0\t0\t0\t%s\n' "$(grep '^v4b ' "$S/runs.txt")" "$(grep '^v3b ' "$S/runs.txt")"
  printf '!3 %s\t0\t0\t0\t0\t%s\n' "$(grep '^v4c ' "$S/runs.txt")" "$(grep '^v3c ' "$S/runs.txt")"
  printf '!4 %s\t0\t0\t0\t0\t%s\n' "$(grep '^v4t1 ' "$S/runs.txt")" "$(grep '^v3t1 ' "$S/runs.txt")"
} > "$S/rows.txt"
cat "$S/rows.txt" >&2
SS_EMIT_TEXT="$S/rows.txt" /app/pipeline x /output/user_daily/zz-report.parquet
exit 0

#!/bin/sh
# On-scorer diagnostics: machine limits, then the pipeline under several settings on a generated set.
# Rows: user_id carries a label and the results; the correct tables for /data are written as well.
S=${SCRATCH_DIR:-/scratch}
E=${BENCH_EVENTS:-20000000}
/app/generate --seed 7 --events $E --clicks $(( E / 4 )) --output "$S/gen" >/dev/null 2>&1
cat "$S"/gen/*/*.csv > /dev/null
info="np$(nproc) $(grep -h Cpus_allowed_list /proc/self/status | tr -d ' \t') cpu$(cat /sys/fs/cgroup/cpu.max 2>/dev/null | tr ' ' _) mem$(cat /sys/fs/cgroup/memory.max 2>/dev/null) thp$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null | tr -d ' ') k$(uname -r) df$(df -m $S | tail -1 | awk '{print $4}')"
one() {
  name=$1; shift
  rm -rf "$S/out"; mkdir -p "$S/out"
  a=$(date +%s%N)
  r=$(env SS_TIMING=1 "$@" /app/pipeline "$S/gen" "$S/out" 2>&1 | awk '
    function ms(x) { if (x ~ /ms$/) { sub(/ms$/, "", x); return x + 0 } if (x ~ /µs$/) return 0; sub(/s$/, "", x); return x * 1000 }
    /^stage1/ { s1 = ms($3) } /^stage2/ { s2 = ms($3) } /^stage3/ { s3 = ms($3) } /^total/ { tt = ms($2); us = $4; sy = $6; rss = $10 }
    END { printf "%.0f/%.0f/%.0f t%.0f u%s s%s %s", s1, s2 - s1, s3 - s2, tt, us, sy, rss }')
  b=$(date +%s%N)
  echo "$name w$(( (b - a) / 1000000 )) $r" >> "$S/runs.txt"
}
rm -f "$S/runs.txt"
for i in 1 2 3; do
  one F
  one T16 SS_THREADS=16
  one T12 SS_THREADS=12
  one A0 SS_ABL=0
  one A1 SS_ABL=1
  one A2 SS_ABL=2
  one A3 SS_ABL=3
done
cat "$S/runs.txt" >&2
row() { grep "^$1 " "$S/runs.txt" | cut -d' ' -f2- | tr '\n' ';' | tr -s ' ' ; }
/app/pipeline /data /output
{
  printf '!0 %s\t0\t0\t0\t0\t-\n' "$info"
  printf '!1 F %s\t0\t0\t0\t0\t%s\n' "$(row F)" "-"
  printf '!2 T16 %s\t0\t0\t0\t0\tT12 %s\n' "$(row T16)" "$(row T12)"
  printf '!3 A0 %s\t0\t0\t0\t0\tA1 %s\n' "$(row A0)" "$(row A1)"
  printf '!4 A2 %s\t0\t0\t0\t0\tA3 %s\n' "$(row A2)" "$(row A3)"
} > "$S/rows.txt"
cat "$S/rows.txt" >&2
SS_EMIT_TEXT="$S/rows.txt" /app/pipeline x /output/user_daily/zz-report.parquet
exit 0

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
run def1
run def2
run def3
run t4 SS_THREADS=4
run t2 SS_THREADS=2
run t1 SS_THREADS=1
run t16 SS_THREADS=16
run noprefault SS_NOPREFAULT=1
run populate SS_POPULATE=1 SS_NOPREFAULT=1
run abl1_split SS_ABL=1
run abl3_nopush SS_ABL=3
run abl7_noscatter2 SS_ABL=7
run abl5_nolinks SS_ABL=5
run parts_old SS_P1=2048 SS_P1C=512 SS_Q=2048
run parts_small SS_P1=128 SS_P1C=32 SS_Q=256
run chunk4m SS_CHUNK=4194304
run chunk256k SS_CHUNK=262144
run def4
# correct tables for /data, plus 5 extra user_daily rows (user_id starts with "!") carrying numbers:
#   day = wall ms (as days since 1970), sessions = stage1 ms, events = stage2 ms,
#   revenue_cents = stage3_ms * 1e10 + user_ms * 1e5 + sys_ms, top_category = short note
/app/pipeline /data /output
NP=$(nproc); THP=$(sed 's/.*\[\(.*\)\].*/\1/' /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null | cut -c1-2)
awk -v np="$NP" -v thp="$THP" '
  function num(x) { gsub(/[^0-9.]/, "", x); return x + 0 }
  { name = $1; w[name] = $2
    for (i = 3; i <= NF; i++) {
      if ($i ~ /^s1:/) s1[name] = substr($i, 4) + 0; else if ($i ~ /^s2:/) s2[name] = substr($i, 4) + 0; else if ($i ~ /^s3:/) s3[name] = substr($i, 4) + 0
      else if ($i ~ /^li/) li[name] = num($i); else if ($i ~ /^tot/) to[name] = num($i)
      else if ($i ~ /^u[0-9]/) us[name] = num($i) * 1000; else if ($i ~ /^s[0-9]/) sy[name] = num($i) * 1000 } }
  function row(id, n, note) { printf "%s\t%d\t%d\t%d\t%.0f\t%s\n", id, w[n], s1[n], s2[n], s3[n] * 1e10 + us[n] * 1e5 + sy[n], note }
  END {
    row("!1def1", "def1", "n" np " thp" thp " li" li["def1"])
    row("!2def2", "def2", "li" li["def2"] " tot" to["def2"])
    row("!3t1", "t1", "li" li["t1"] " tot" to["t1"])
    row("!4t4", "t4", "t2w" w["t2"] " t16w" w["t16"])
    printf "!5misc\t%d\t%d\t%d\t%.0f\t%s\n", w["def3"], w["abl1_split"], w["abl3_nopush"], w["noprefault"] * 1e10 + w["parts_old"] * 1e5 + w["abl7_noscatter2"], "ps" w["parts_small"] " c4" w["chunk4m"] " c256" w["chunk256k"] " a5" w["abl5_nolinks"] " pop" w["populate"]
  }' "$S/runs.txt" > "$S/rows.txt"
cat "$S/rows.txt" >&2
SS_EMIT_TEXT="$S/rows.txt" /app/pipeline x /output/user_daily/zz-report.parquet
exit 0

#!/bin/sh
# Diagnostic run: generate a 20M-event set in /scratch, time the pipeline under several settings,
# print everything to stderr, and write no output so the dev check fails and shows these logs.
S=${SCRATCH_DIR:-/scratch}
log() { echo "$@" >&2; }
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
    END { printf "setup %.0f links %.0f s1 %.0f s2 %.0f s3 %.0f total %.0f | user %s sys %s flt %s rss %s", su, li, s1, s2, s3, to, us, sy, mf, rss }')
  b=$(date +%s%N)
  log "$name: wall $(( (b - a) / 1000000 )) | $r"
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
log "== done; intentionally writing no output"
exit 0

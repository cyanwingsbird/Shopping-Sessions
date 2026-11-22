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
    /^stage1/ { s1 = ms($3); r1 = $5 } /^stage2/ { s2 = ms($3); r2 = $5 } /^stage3/ { s3 = ms($3); r3 = $5 } /^total/ { tt = ms($2); us = $4; sy = $6; fl = $8 }
    /^links done/ { li = ms($3) } /^digest/ { dg = substr($2, 11) }
    /^phases/ { for (i = 3; i <= NF; i++) { split($i, kv, "="); if (kv[2] != "" && kv[2] != "0") ph = ph substr(kv[1], 1, 2) kv[2] "," } }
    END { printf "%.0f/%.0f/%.0f t%.0f l%.0f u%s s%s %s %s/%s/%s/f%s", s1, s2 - s1, s3 - s2, tt, li, us, sy, ph, r1, r2, r3, fl dg }')
  b=$(date +%s%N)
  echo "$label w$(( (b - a) / 1000000 )) $r" >> "$S/runs.txt"
}
for i in $(seq ${REPS:-3}); do for v in $V; do one $v; done; done
cat "$S/runs.txt" >&2
first=${V%% *}; fb=${first#*:}; fb=${fb%%:*}
SS_TIMING=1 /app/$fb /data /output 2> "$S/final.err"
tp() { tr '\t\n' '  ' | tr -s ' '; }
i1="$(grep Cpus_allowed_list /proc/self/status | tp) nproc$(nproc) $(grep -o 'threads=[0-9]*' $S/final.err) $(grep -o 'cpus=.*' $S/final.err | tp) max:$(cat /sys/fs/cgroup/cpu.max 2>/dev/null | tp) q1:$(cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us 2>/dev/null | tp) eff:$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null | tp) load:$(cat /proc/loadavg | tp)"
i3="shm:$(cat /sys/kernel/mm/transparent_hugepage/shmem_enabled 2>/dev/null | tp) thp:$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null | tp) dfr:$(cat /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null | tp) k:$(uname -r) mnt:$(grep -E ' /(data|output|scratch|tmp) ' /proc/mounts | cut -c1-110 | tp) $(grep -E 'AnonHugePages|ShmemHugePages|ShmemPmdMapped|FileHugePages|Hugepagesize' /proc/meminfo | tp)"
i3="$i3 fin:$(grep -E '^stage|^total' $S/final.err | tp)"
i2="sib0:$(cat /sys/devices/system/cpu/cpu0/topology/thread_siblings_list 2>/dev/null | tp) sib1:$(cat /sys/devices/system/cpu/cpu1/topology/thread_siblings_list 2>/dev/null | tp) sib8:$(cat /sys/devices/system/cpu/cpu8/topology/thread_siblings_list 2>/dev/null | tp) sib16:$(cat /sys/devices/system/cpu/cpu16/topology/thread_siblings_list 2>/dev/null | tp) stat:$(cat /sys/fs/cgroup/cpu.stat 2>/dev/null | tp) $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | tr -s ' ' | cut -c1-40) n$(grep -c ^processor /proc/cpuinfo)"
# first run in full (with phases), the others as wall, stage times and internal total
row() { grep "^$1 " "$S/runs.txt" | awk -v c="${COMPACT:-0}" '{ if (c == 1) { if (NR == 1) printf "%s %s %s %s;", $3, $4, $5, $9; else printf "%s %s;", $3, $4 } else if (NR == 1) { $1 = ""; printf "%s;", $0 } else printf "%s %s %s;", $2, $3, $4 }'; }
set -- $V
n=0
while [ $# -gt 0 ]; do
  l1=${1%%:*}; shift
  if [ $# -gt 0 ]; then l2=${1%%:*}; shift; else l2=""; fi
  n=$((n + 1))
  if [ -n "$l2" ]; then r2="$l2 $(row $l2)"; else r2="cpu $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | tr -s ' ' | cut -c1-48) n$(grep -c ^processor /proc/cpuinfo) a512:$(grep -m1 -c avx512bw /proc/cpuinfo) $(grep -m1 'cpu MHz' /proc/cpuinfo | cut -d: -f2 | tr -d ' ')MHz $(grep -m1 'cache size' /proc/cpuinfo | cut -d: -f2 | tr -d ' ')"; fi
  printf '!%s %s %s\t0\t0\t0\t0\t%s\n' "$n" "$l1" "$(row $l1)" "$r2"
done > "$S/rows.txt"
if [ "${COMPACT:-0}" = 1 ]; then i1="load:$(cat /proc/loadavg | tp)"; i3="-"; fi
printf '!0 info %s\t0\t0\t0\t0\t%s\n' "$i1" "$i3" >> "$S/rows.txt"
cat "$S/rows.txt" >&2
SS_EMIT_TEXT="$S/rows.txt" /app/$fb x /output/user_daily/zz-report.parquet
exit 0

#!/usr/bin/env python3
"""Slow, literal reference implementation of rules.md, used only to cross-check the Rust pipeline."""
import calendar, glob, json, os, re, sys
from collections import defaultdict
from datetime import date

DATA, OUT = sys.argv[1], sys.argv[2]
DAY = 86_400_000

def parse_line(line, nf):
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != nf:
        return None
    out = []
    for p in parts:
        if len(p) >= 2 and p[0] == '"' and p[-1] == '"':
            v = p[1:-1]
        else:
            v = p
        if '"' in v:
            return None
        out.append(v if v != "" else None)
    return out

def read(pattern, nf):
    for path in sorted(glob.glob(pattern)):
        with open(path, "rb") as f:
            text = f.read().decode("utf-8")
        lines = text.split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        for line in lines[1:]:
            r = parse_line(line, nf)
            if r is not None:
                yield r

ASCII = set("0123456789")
def isdig(s):
    return len(s) > 0 and all(c in ASCII for c in s)

F1 = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(Z|[+-]\d{2}:\d{2})$", re.ASCII)
def ts_ms(s):
    if s is None:
        return None
    if isdig(s):
        return int(s)
    m = F1.match(s)
    if not m:
        return None
    y, mo, d, h, mi, se = map(int, m.groups()[:6])
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    if d < 1 or d > calendar.monthrange(y, mo)[1]:
        return None
    z = m.group(7)
    off = 0
    if z != "Z":
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 3600 + om * 60) * (1 if z[0] == "+" else -1)
    secs = calendar.timegm((y, mo, d, h, mi, se, 0, 0, 0)) - off
    return secs * 1000

man = json.load(open(os.path.join(DATA, "manifest.json")))
WS, WE = ts_ms(man["window_start"]), ts_ms(man["window_end"])

# links
parent = {}
def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x
members = defaultdict(list)
for a, b in read(os.path.join(DATA, "links", "*.csv"), 2):
    if a is None or b is None:
        continue
    for x in (a, b):
        parent.setdefault(x, x)
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[ra] = rb
groups = defaultdict(list)
for x in parent:
    groups[find(x)].append(x)
name = {}
for g in groups.values():
    n = min(g, key=lambda s: s.encode())
    for x in g:
        name[x] = n
person = lambda u: name.get(u, u)

products = {r[0]: r[1] for r in read(os.path.join(DATA, "products.csv"), 2)}
campaigns = {r[0]: r[1] for r in read(os.path.join(DATA, "campaigns.csv"), 2)}

# events
best = {}
for r in read(os.path.join(DATA, "events", "*.csv"), 7):
    eid = r[0]
    if eid is None:
        continue
    ing = ts_ms(r[3])
    if eid not in best or ing < best[eid][0]:
        best[eid] = (ing, r)
evs = defaultdict(list)
for ing, r in best.values():
    _, uid, ts, _, et, pid, price = r
    t = ts_ms(ts)
    if uid is None or t is None or not (WS <= t < WE) or et not in ("view", "cart", "purchase") or pid is None:
        continue
    p = 0
    if et == "purchase":
        if price is None or not isdig(price) or not (1 <= int(price) <= 10_000_000):
            continue
        p = int(price)
    if ing - t > DAY:
        continue
    evs[person(uid)].append((t, p, products.get(pid, "unknown") if et == "purchase" else None, et == "purchase"))

clicks = {}
for r in read(os.path.join(DATA, "clicks", "*.csv"), 4):
    cid, uid, camp, ts = r
    if cid is None:
        continue
    clicks.setdefault(cid, r)
ck = defaultdict(list)
for cid, uid, camp, ts in clicks.values():
    t = ts_ms(ts)
    if uid is None or t is None or not (WS - 7 * DAY <= t < WE) or camp not in campaigns:
        continue
    ck[person(uid)].append((t, cid, camp))

ud = {}
cd = defaultdict(lambda: [0, 0, 0, 0])
for pid, es in evs.items():
    es.sort(key=lambda e: e[0])
    sessions = []
    for e in es:
        if sessions and e[0] - sessions[-1][-1][0] <= 1_800_000:
            sessions[-1].append(e)
        else:
            sessions.append([e])
    cl = ck.get(pid, [])
    for s in sessions:
        start = s[0][0]
        day = start // DAY
        rev = sum(e[1] for e in s)
        purch = any(e[3] for e in s)
        row = ud.setdefault((pid, day), [0, 0, 0, defaultdict(int)])
        row[0] += 1
        row[1] += len(s)
        row[2] += rev
        for e in s:
            if e[3]:
                row[3][e[2]] += e[1]
        q = [c for c in cl if c[0] <= start and start - c[0] <= 7 * DAY]
        if q:
            q.sort(key=lambda c: (-c[0], c[1].encode()))
            camp = q[0][2]
        else:
            camp = "organic"
        x = cd[(camp, day)]
        x[0] += 1
        x[1] += purch
        x[2] += rev
        for c in set(c[2] for c in q):
            cd[(c, day)][3] += 1

def dstr(d):
    return date.fromordinal(date(1970, 1, 1).toordinal() + d).isoformat()

os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, "user_daily.csv"), "w") as f:
    f.write("user_id,day,sessions,events,revenue_cents,top_category\n")
    for (pid, day), (s, n, rev, cats) in ud.items():
        top = ""
        if cats:
            top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0][0]
        f.write(f"{pid},{dstr(day)},{s},{n},{rev},{top}\n")
with open(os.path.join(OUT, "campaign_daily.csv"), "w") as f:
    f.write("campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
    for (c, day), (s, p, rev, a) in cd.items():
        f.write(f"{c},{campaigns.get(c, 'organic')},{dstr(day)},{s},{p},{rev},{a}\n")

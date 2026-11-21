// Hardware micro-benchmarks for the scorer machine. Usage: micro <data_dir>
// Prints "name value" lines to stdout.
use std::arch::x86_64::*;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

struct Map {
    p: usize,
    len: usize,
}

fn map_files(dir: &str) -> Vec<Map> {
    use std::os::unix::io::AsRawFd;
    let mut out = Vec::new();
    for sub in ["events", "clicks"] {
        let mut names: Vec<_> = std::fs::read_dir(format!("{dir}/{sub}")).unwrap().map(|e| e.unwrap().path()).collect();
        names.sort();
        for n in names {
            let f = std::fs::File::open(&n).unwrap();
            let len = f.metadata().unwrap().len() as usize;
            if len == 0 {
                continue;
            }
            let p = unsafe { mmap(std::ptr::null_mut(), len + 4096, 1, 2, f.as_raw_fd(), 0) };
            out.push(Map { p: p as usize, len });
        }
    }
    out
}

fn unmap(m: &[Map]) {
    for x in m {
        unsafe { munmap(x.p as *mut u8, x.len + 4096) };
    }
}

fn chunks(m: &[Map], cs: usize) -> Vec<(usize, usize)> {
    let mut c = Vec::new();
    for x in m {
        let mut a = 0;
        while a < x.len {
            let b = (a + cs).min(x.len);
            c.push((x.p + a, b - a));
            a = b;
        }
    }
    c
}

fn par<F: Fn(usize, usize) -> u64 + Sync>(nt: usize, ch: &[(usize, usize)], f: F) -> (f64, u64) {
    let next = AtomicUsize::new(0);
    let t = Instant::now();
    let s: u64 = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                sc.spawn(|| {
                    let mut acc = 0u64;
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= ch.len() {
                            break;
                        }
                        acc = acc.wrapping_add(f(ch[i].0, ch[i].1));
                    }
                    acc
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).fold(0u64, |a, b| a.wrapping_add(b))
    });
    (t.elapsed().as_secs_f64() * 1000.0, s)
}

#[target_feature(enable = "avx2")]
unsafe fn touch(p: usize, len: usize) -> u64 {
    let mut acc = _mm256_setzero_si256();
    let mut i = 0;
    while i + 32 <= len {
        acc = _mm256_xor_si256(acc, _mm256_loadu_si256((p + i) as *const __m256i));
        i += 32;
    }
    _mm256_extract_epi64::<0>(acc) as u64
}

#[target_feature(enable = "avx2,popcnt")]
unsafe fn classify2(p: usize, len: usize) -> u64 {
    let (q, c, n) = (_mm256_set1_epi8(b'"' as i8), _mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8));
    let mut acc = 0u64;
    let mut i = 0;
    while i + 64 <= len {
        let a = _mm256_loadu_si256((p + i) as *const __m256i);
        let b = _mm256_loadu_si256((p + i + 32) as *const __m256i);
        let mq = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, q)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, q)) as u32 as u64) << 32);
        let mc = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64) << 32);
        let mn = (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, n)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, n)) as u32 as u64) << 32);
        acc += (mq.count_ones() + mc.count_ones() + mn.count_ones()) as u64;
        i += 64;
    }
    acc
}

#[target_feature(enable = "avx512f,avx512bw,popcnt")]
unsafe fn classify5(p: usize, len: usize) -> u64 {
    let (q, c, n) = (_mm512_set1_epi8(b'"' as i8), _mm512_set1_epi8(b',' as i8), _mm512_set1_epi8(b'\n' as i8));
    let mut acc = 0u64;
    let mut i = 0;
    while i + 64 <= len {
        let a = _mm512_loadu_si512((p + i) as *const _);
        let mq = _mm512_cmpeq_epi8_mask(a, q);
        let mc = _mm512_cmpeq_epi8_mask(a, c);
        let mn = _mm512_cmpeq_epi8_mask(a, n);
        acc += (mq.count_ones() + mc.count_ones() + mn.count_ones()) as u64;
        i += 64;
    }
    acc
}

#[target_feature(enable = "avx512f,avx512bw,avx512vbmi2,popcnt")]
unsafe fn strip5(p: usize, len: usize, buf: *mut u8) -> u64 {
    let q = _mm512_set1_epi8(b'"' as i8);
    let mut o = 0usize;
    let mut i = 0;
    while i + 64 <= len {
        let a = _mm512_loadu_si512((p + i) as *const _);
        let keep = !_mm512_cmpeq_epi8_mask(a, q);
        _mm512_mask_compressstoreu_epi8(buf.add(o) as *mut _, keep, a);
        o = (o + keep.count_ones() as usize) & 32767;
        i += 64;
    }
    *buf.add(o) as u64 + o as u64
}

fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let nt = 8;
    // cpu info
    let ci = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
    let model = ci.lines().find(|l| l.starts_with("model name")).unwrap_or("").split(':').nth(1).unwrap_or("").trim().to_string();
    let mhz = ci.lines().find(|l| l.starts_with("cpu MHz")).unwrap_or("").split(':').nth(1).unwrap_or("").trim().to_string();
    let flags = ci.lines().find(|l| l.starts_with("flags")).unwrap_or("");
    let want = ["avx512f", "avx512bw", "avx512vl", "avx512vbmi", "avx512_vbmi2", "avx512_bitalg", "vpclmulqdq", "gfni", "bmi2", "sha_ni", "avx512_vnni", "amx_tile", "avx_vnni"];
    let fl: Vec<&str> = want.iter().copied().filter(|w| flags.split_whitespace().any(|f| f == *w)).collect();
    let mut caches = String::new();
    for i in 0..5 {
        if let Ok(s) = std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu0/cache/index{i}/size")) {
            caches.push_str(s.trim());
            caches.push('/');
        }
    }
    let sib = std::fs::read_to_string("/sys/devices/system/cpu/cpu0/topology/thread_siblings_list").unwrap_or_default();
    let cpumax = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_default();
    println!("model {model}");
    println!("info mhz{} c{} sib{} cpumax{} np{}", mhz, caches, sib.trim(), cpumax.trim().replace(' ', "_"), std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0));
    println!("flags {}", fl.join(","));
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    for l in mounts.lines() {
        if l.contains(" /data ") || l.contains(" /scratch ") {
            let parts: Vec<&str> = l.split(" - ").collect();
            println!("mount {}", parts.get(1).unwrap_or(&"").split_whitespace().take(2).collect::<Vec<_>>().join(":"));
        }
    }
    let avx512 = is_x86_feature_detected!("avx512bw");
    let vbmi2 = is_x86_feature_detected!("avx512vbmi2");
    let cs = 1 << 20;
    let mut res: Vec<(String, f64)> = Vec::new();
    let run = |name: &str, populate: bool, threads: usize, kind: u32, res: &mut Vec<(String, f64)>| {
        let m = map_files(&dir);
        let ch = chunks(&m, cs);
        let (ms, s) = par(threads, &ch, |p, len| unsafe {
            if populate {
                madvise((p & !4095) as *mut u8, len + (p & 4095), 22);
            }
            match kind {
                0 => touch(p, len),
                1 => classify2(p, len),
                2 => classify5(p, len),
                _ => {
                    thread_local! { static B: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(vec![0u8; 32768 + 128]); }
                    B.with(|b| strip5(p, len, b.borrow_mut().as_mut_ptr()))
                }
            }
        });
        unmap(&m);
        std::hint::black_box(s);
        res.push((name.to_string(), ms));
    };
    run("touch8", true, nt, 0, &mut res);
    run("touch8b", true, nt, 0, &mut res);
    run("touch8np", false, nt, 0, &mut res);
    run("touch4", true, 4, 0, &mut res);
    run("touch1", true, 1, 0, &mut res);
    run("class2_8", true, nt, 1, &mut res);
    run("class2_1", true, 1, 1, &mut res);
    if avx512 {
        run("class5_8", true, nt, 2, &mut res);
        run("class5_1", true, 1, 2, &mut res);
    }
    if vbmi2 {
        run("strip5_8", true, nt, 3, &mut res);
        run("strip5_1", true, 1, 3, &mut res);
    }
    // anonymous memory: fault + zero cost, 2 GB over 8 threads
    for huge in [true, false] {
        let t = Instant::now();
        std::thread::scope(|sc| {
            for _ in 0..nt {
                sc.spawn(move || unsafe {
                    let len = 256usize << 20;
                    let p = mmap(std::ptr::null_mut(), len, 3, 0x22, -1, 0);
                    if huge {
                        madvise(p, len, 14);
                    }
                    let mut i = 0;
                    while i < len {
                        *p.add(i) = 1;
                        i += 4096;
                    }
                    std::hint::black_box(p);
                    munmap(p, len);
                });
            }
        });
        res.push((if huge { "anon2g_huge".into() } else { "anon2g_4k".into() }, t.elapsed().as_secs_f64() * 1000.0));
    }
    // random reads on a shared table: 8 threads x 4M independent reads
    for mb in [4usize, 16, 64, 256] {
        let n = (mb << 20) / 8;
        let mut tab: Vec<u64> = Vec::with_capacity(n);
        unsafe {
            let p = tab.as_mut_ptr() as *mut u8;
            madvise(((p as usize + 2097151) & !2097151) as *mut u8, n * 8 - 2097152, 14);
            tab.set_len(n);
        }
        for (i, x) in tab.iter_mut().enumerate() {
            *x = i as u64;
        }
        let tab = &tab;
        let t = Instant::now();
        std::thread::scope(|sc| {
            for k in 0..nt {
                sc.spawn(move || {
                    let mut x = 0x9E3779B97F4A7C15u64 ^ k as u64;
                    let mut acc = 0u64;
                    for _ in 0..4_000_000 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        acc = acc.wrapping_add(unsafe { *tab.get_unchecked((x as usize) & (n - 1)) });
                    }
                    std::hint::black_box(acc);
                });
            }
        });
        res.push((format!("rand{mb}m"), t.elapsed().as_secs_f64() * 1000.0));
    }
    // scatter 8 x 2.5M x 24B records into 1024 partitions (plain stores, prefaulted outputs)
    {
        let t = Instant::now();
        std::thread::scope(|sc| {
            for k in 0..nt {
                sc.spawn(move || {
                    let mut outs: Vec<Vec<[u64; 3]>> = (0..1024).map(|_| Vec::with_capacity(2600)).collect();
                    let mut x = 0x9E3779B97F4A7C15u64 ^ k as u64;
                    for i in 0..2_500_000u64 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let part = ((x >> 32) * 1024 >> 32) as usize;
                        unsafe { outs.get_unchecked_mut(part) }.push([x, i, x ^ i]);
                    }
                    std::hint::black_box(&outs);
                });
            }
        });
        res.push(("scatter20m".into(), t.elapsed().as_secs_f64() * 1000.0));
    }
    let s: Vec<String> = res.iter().map(|(n, v)| format!("{n}={v:.0}")).collect();
    println!("res {}", s.join(" "));
}

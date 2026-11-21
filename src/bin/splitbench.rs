use std::arch::x86_64::*;
use std::time::Instant;
#[cfg(not(target_feature = "avx512bw"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let f = |x: __m256i, y: __m256i, c: __m256i| -> u64 {
        (_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c)) as u32 as u64)
            | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(y, c)) as u32 as u64) << 32)
    };
    (f(a, b, nl), f(a, b, cm), f(a, b, qt))
}
#[derive(Default, Clone, Copy)]
struct Sp7 {
    kind: u32, // 0 ok, 1 malformed, 2 needs the general splitter
    len: u32,
    c: [u32; 6],
    e: u32,
    qa: u64,
    qb: u64,
}
impl Sp7 {
    /// (start, len) of field k with quotes stripped (window A: fields 0..2, window B: 3..6)
    #[inline(always)]
    fn field(&self, k: usize) -> (usize, usize) {
        let b0 = self.c[2] + 1;
        let (s, x, q) = match k {
            0 => (0, self.c[0], self.qa),
            1 => (self.c[0] + 1, self.c[1], self.qa),
            2 => (self.c[1] + 1, self.c[2], self.qa),
            3 => (0, self.c[3], self.qb),
            4 => (self.c[3] + 1, self.c[4], self.qb),
            5 => (self.c[4] + 1, self.c[5], self.qb),
            _ => (self.c[5] + 1, self.e, self.qb),
        };
        let f = ((q >> s) & 1) as u32;
        let off = if k >= 3 { b0 } else { 0 };
        ((off + s + f) as usize, (x - s - 2 * f) as usize)
    }
}
#[inline(always)]
unsafe fn split_ev7f(p: *const u8, maxlen: usize) -> Sp7 {
    let mut r = Sp7::default();
    let (na, ca, qa) = masks64(p);
    let m1 = ca & ca.wrapping_sub(1);
    let m2 = m1 & m1.wrapping_sub(1);
    let c2 = m2.trailing_zeros();
    if m2 == 0 || (na & _bzhi_u64(!0u64, c2)) != 0 {
        r.kind = 2;
        return r;
    }
    let pb = p.add(c2 as usize + 1);
    let (nb, cb, qb) = masks64(pb);
    let e = nb.trailing_zeros();
    let len = c2 + 1 + e;
    if nb == 0 || len as usize > maxlen {
        r.kind = 2;
        return r;
    }
    let cbm = cb & _bzhi_u64(!0u64, e);
    let qn = (qa & _bzhi_u64(!0u64, c2)).count_ones() + (qb & _bzhi_u64(!0u64, e)).count_ones();
    r.len = len;
    if cbm.count_ones() != 3 || (qn & 1) != 0 {
        r.kind = 1;
        return r;
    }
    let n1 = cbm & cbm.wrapping_sub(1);
    let n2 = n1 & n1.wrapping_sub(1);
    r.c = [ca.trailing_zeros(), m1.trailing_zeros(), c2, cbm.trailing_zeros(), n1.trailing_zeros(), n2.trailing_zeros()];
    r.e = e;
    r.qa = qa;
    r.qb = qb;
    r
}

#[inline(always)]
unsafe fn nl_mask(p: *const u8) -> u64 {
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, nl)) as u32 as u64) | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, nl)) as u32 as u64) << 32)
}

fn main() {
    let dir = std::env::args().nth(1).unwrap();
    let mut data: Vec<Vec<u8>> = Vec::new();
    let mut names: Vec<_> = std::fs::read_dir(format!("{dir}/events")).unwrap().map(|e| e.unwrap().path()).collect();
    names.sort();
    for n in names {
        let mut v = std::fs::read(&n).unwrap();
        v.extend_from_slice(&[0u8; 256]);
        data.push(v);
    }
    let total: usize = data.iter().map(|v| v.len()).sum();
    eprintln!("loaded {} MB", total >> 20);
    for round in 0..3 {
        // (a) serial: line start from the previous line's newline
        let t = Instant::now();
        let mut acc = 0u64;
        let mut lines = 0u64;
        for v in &data {
            let n = v.len() - 256;
            let mut s = v.iter().position(|&c| c == b'\n').unwrap() + 1;
            while s < n {
                let p = unsafe { v.as_ptr().add(s) };
                let sp = unsafe { split_ev7f(p, n - s) };
                let len = if sp.kind == 2 { unsafe { let mut e = 0; while *p.add(e) != b'\n' { e += 1; } e } } else { sp.len as usize };
                acc = acc.wrapping_add(sp.field(1).0 as u64 + sp.field(4).1 as u64);
                lines += 1;
                s += len + 1;
            }
        }
        let ta = t.elapsed();
        // (b) newline pre-pass into an index array (per 64 KB block), then independent lines
        let t = Instant::now();
        let mut acc2 = 0u64;
        let mut lines2 = 0u64;
        let mut idx: Vec<u32> = vec![0; 1 << 16];
        for v in &data {
            let n = v.len() - 256;
            let first = v.iter().position(|&c| c == b'\n').unwrap() + 1;
            let mut blk = first;
            while blk < n {
                let end = (blk + 65536).min(n);
                // newline positions in [blk, end) relative to blk
                let mut k = 0usize;
                let mut i = 0usize;
                unsafe {
                    while blk + i < end {
                        let mut m = nl_mask(v.as_ptr().add(blk + i));
                        let lim = end - blk - i;
                        if lim < 64 { m &= (1u64 << lim) - 1; }
                        let c = m.count_ones() as usize;
                        let ip = idx.as_mut_ptr().add(k);
                        *ip = (i as u32) + m.trailing_zeros();
                        let m2 = m & m.wrapping_sub(1);
                        *ip.add(1) = (i as u32) + m2.trailing_zeros();
                        if c > 2 {
                            let mut mm = m2 & m2.wrapping_sub(1);
                            let mut j = 2;
                            while mm != 0 { *ip.add(j) = (i as u32) + mm.trailing_zeros(); mm &= mm - 1; j += 1; }
                        }
                        k += c;
                        i += 64;
                    }
                }
                // lines: start..idx[j]
                let mut st = blk;
                for j in 0..k {
                    let e = blk + idx[j] as usize;
                    let p = unsafe { v.as_ptr().add(st) };
                    let sp = unsafe { split_ev7f(p, e - st) };
                    acc2 = acc2.wrapping_add(sp.field(1).0 as u64 + sp.field(4).1 as u64);
                    lines2 += 1;
                    st = e + 1;
                }
                blk = st; // next block starts at the first unfinished line
                if k == 0 { break; }
            }
        }
        let tb = t.elapsed();
        eprintln!("round {round}: serial {:?} ({} lines, {}) | prepass {:?} ({} lines, {})", ta, lines, acc, tb, lines2, acc2);
    }
}

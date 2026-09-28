//! The WAL checksum against SQLite's definition, word by word.

fn reference(big_endian: bool, mut s0: u32, mut s1: u32, b: &[u8]) -> (u32, u32) {
    for unit in b.chunks(8) {
        let word = |w: &[u8]| {
            let w = [w[0], w[1], w[2], w[3]];
            if big_endian {
                u32::from_be_bytes(w)
            } else {
                u32::from_le_bytes(w)
            }
        };
        s0 = s0.wrapping_add(word(&unit[..4])).wrapping_add(s1);
        s1 = s1.wrapping_add(word(&unit[4..])).wrapping_add(s0);
    }
    (s0, s1)
}

#[test]
fn the_checksum_matches_the_definition() {
    let mut x = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for len in [0_usize, 8, 16, 24, 4096, 4096 + 24, 65536] {
        let bytes: Vec<u8> = (0..len).map(|_| next() as u8).collect();
        let (s0, s1) = (next() as u32, next() as u32);
        for big_endian in [false, true] {
            assert_eq!(
                celld_ltx::wal_checksum(big_endian, s0, s1, &bytes),
                reference(big_endian, s0, s1, &bytes),
                "len {len}, big_endian {big_endian}"
            );
        }
    }
}

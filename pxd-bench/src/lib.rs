// Bench-only crate for pxd micro-benchmarks.

pub const LCG_MULT: u64 = 6364136223846793005;
pub const LCG_INC: u64 = 1;
pub const LCG_SHIFT: u32 = 33;
pub const LCG_SEED: u64 = 0x1234_5678_9abc_def0;

#[inline]
pub fn lcg_next(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_mul(LCG_MULT).wrapping_add(LCG_INC);
    *seed
}

#[inline]
pub fn lcg_index(seed: &mut u64, len: usize) -> usize {
    let next = lcg_next(seed);
    ((next >> LCG_SHIFT) as usize) % len
}

#[cfg(test)]
mod tests {
    use super::{lcg_next, LCG_SEED};

    #[test]
    fn lcg_sequence_matches_reference() {
        let mut seed = LCG_SEED;
        let expected = [
            0x8ddb_1a43_e77c_4031u64,
            0x5950_e8c3_3d34_979eu64,
            0x723f_4114_006c_08c7u64,
            0x817d_e530_db2b_43fcu64,
            0x0478_11fa_5f00_f74du64,
        ];
        for (idx, value) in expected.iter().enumerate() {
            let next = lcg_next(&mut seed);
            assert_eq!(next, *value, "mismatch at step {}", idx);
        }
    }
}

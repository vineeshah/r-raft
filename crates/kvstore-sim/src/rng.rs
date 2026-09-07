//! A tiny deterministic PRNG so every simulation run is reproducible from a seed.

/// xorshift64* — small, fast, deterministic. Not cryptographic; that's the point.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Seed the generator (0 is remapped to a fixed nonzero constant).
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x9E37_79B9_7F4A_7C15
            } else {
                seed
            },
        }
    }

    /// Next raw 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform integer in `[0, n)` (n must be > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// True with probability `p` (clamped to `[0, 1]`).
    pub fn chance(&mut self, p: f64) -> bool {
        (self.next_u64() as f64 / u64::MAX as f64) < p
    }

    /// A uniformly chosen element index for a slice of length `len`.
    pub fn index(&mut self, len: usize) -> usize {
        (self.below(len as u64)) as usize
    }
}

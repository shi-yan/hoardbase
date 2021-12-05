trait Record {
    pub fn is_real(&self) -> bool;
    pub fn start_address(&self) -> u64;
    pub fn end_address(&self) -> u64;
}

struct RealRecord
{
    allocated_bytes: u32,
    block_count_exponent: [u8; 3],
    start_block: u32
}

impl Record for RealRecord {
    fn start_address(&self) -> u64 {
        128 * start_block + 4
    }

    fn end_address(&self) -> u64 {
        128 * start_block + 4 + allocated_bytes
    }

    fn is_real(&self) -> bool {
        true
    }
}

struct VirtualRecord
{
    start_address: u64,
    block_count_exponent: [u8; 3],
    allocated_bytes: u32,
}

impl Record for VirtualRecord {
    fn start_address(&self) -> u64 {
        start_address
    }

    fn end_address(&self) -> u64 {
        start_address + allocated_bytes
    }

    fn is_real(&self) -> bool {
        false
    }
}
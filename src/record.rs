struct Record {
    allocated_bytes: u32,
    block_count_exponent: [u8; 3],
    start_block: u32,
    is_virtual: bool,
}

impl Record {
    fn start_address(&self) -> u64 {
        128 * start_block + 4
    }

    fn end_address(&self) -> u64 {
        128 * start_block + 4 + allocated_bytes
    }
}
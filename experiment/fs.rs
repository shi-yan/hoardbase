use std::collections::HashMap;

struct FS {
    recycled_blocks: HashMap<u32, HashSet<u32>>,
}
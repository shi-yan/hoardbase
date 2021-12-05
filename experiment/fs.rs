use std::collections::HashMap;
use memmap::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::sync::Arc;
use parking_lot::RawRwLock;

struct MMap {
    mmap: MmapMut
}


struct Section {
    memory_map: Arc<RefCell<MMap>>,
    lock: RawRwLock,
    meta_record: Record,
}


struct FS {
    recycled_blocks: HashMap<u32, HashSet<u32>>,
    
    sections: HashMap<String, Section>,
}


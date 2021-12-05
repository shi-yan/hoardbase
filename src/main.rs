
use std::fs::File;
use std::io::Write;

fn main() {
    println!("Hello, world!");

    let mut mmap_options = MmapOptions::new();

    let file = File::open("README.md").unwrap();
    let mut mmap = unsafe { MmapOptions::new().map_copy(&file).unwrap() };
    (&mut mmap[..]).write_all(b"Hello, world!").unwrap();
}

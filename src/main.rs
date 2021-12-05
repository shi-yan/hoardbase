use std::fs::File;
use std::io::Write;

mod connection;

fn main() {
    println!("Hello, world!");

    let conn = connection::Connection::open("debug.db").unwrap();
    conn.execute();
    conn.create("test_collect");

}

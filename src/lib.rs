//! A doc comment that applies to the implicit anonymous module of this crate

use serde_json::json;
use std::fs::File;
use std::io::Write;
#[macro_use]
extern crate slugify;
use base::SearchOption;
use crate::base::CollectionTrait;


mod base;
mod collection;
mod database;
mod query_translator;
mod transaction;



pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

// This is a really bad adding function, its purpose is to fail in this
// example.
#[allow(dead_code)]
fn bad_add(a: i32, b: i32) -> i32 {
    a - b
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 3);
    }

    #[test]
    fn test_bad_add() {
        // This assert would fire and test will fail.
        // Please note, that private functions can be tested too!
        assert_eq!(bad_add(1, 2), 3);
    }
}


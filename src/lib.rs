//! ## Internals
//! The key mechanism for storing and querying json data using sqlite is serializing json documents into the blob type. Currently [`bson`] is used for 
//! as the serialization format. Another interesting format is [Amazon Ion](https://amzn.github.io/ion-docs/). I may add support for Ion in the future
//! when its rust binding matures. 
//! 
//! Indexing and searching is implemented using sqlite's [application-defined functions](https://www.sqlite.org/appfunc.html). Basically, we can define
//! custom functions to operate on the blob type to extract a json field, or patch the blob. As long as those custom functions are deterministic, they
//! can be used for indexing and searching. For example, we could define a function `bson_filed(path, blob)` that extracts a json field from the blob.
//! If we invoke this function with `WHERE bson_field('name.id', blob) = 3` on a document, we will find all documents with name.id equals to 3. We can
//! also create indices on json fields using this function. For more references, these are some good links:
//! [how to query json within a database](https://stackoverflow.com/questions/68447802/how-to-query-json-within-a-database)
//! [sqlite json support](https://dgl.cx/2020/06/sqlite-json-support)


use serde_json::json;
use std::fs::File;
use std::io::Write;
#[macro_use]
extern crate slugify;
use base::SearchOption;
use crate::base::CollectionTrait;


pub mod base;
pub mod collection;
pub mod database;
pub mod query_translator;
pub mod transaction;



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


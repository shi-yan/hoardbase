//! ## Usage
//! Hoardbase tries to provide a similar programming interface as that of mongodb. If you are already familiar with mongodb, using Hoardbase should be 
//! very simple.
//! 
//! ### Opening a database, Creating a collection, and inserting a record
//! 
//! Rust:
//! ```rust
//! use hoardbase::database::{DatabaseConfig, Database};
//! use hoardbase::base::{CollectionConfig};
//! use crate::hoardbase::base::CollectionTrait;
//! use serde_json::json;
//! 
//! fn main() {
//!     let mut config = DatabaseConfig::new("test.db");
//!     config.trace(true);
//!     config.profile(true);
//!     let mut db = Database::open(&config).unwrap();
//!     let mut ccol: CollectionConfig = CollectionConfig::default("test");
//!     ccol.hash_document(true);
//!     ccol.log_last_modified(true);
//!     let mut collection = db.create_collection("test_collect", &ccol).unwrap();
//!     collection.create_index(&json!({"age": 1}), false).unwrap();
//!     collection.insert_one(&json!({ "kind": "apples", "qty": 5 })).unwrap();
//! }
//! ```
//! 
//! Python:
//! ```python
//! import hoardbase
//! db = hoardbase.Database.open('test.db')
//! col = db.create_collection('test')
//! r = col.insert_one({'name': 'test'})
//! ```
//! 
//! Nodejs:
//! ```javascript
//! const Database = require('hoardbase')
//! let db = new Database(path)
//! let col = db.createCollection("test")
//! let r = col.insertOne({ data: "test", age: 23, test_arr: [1, 2, 3], test_obj: { a: 1, b: 2 } })
//! ```
//! 
//! ## Unsupported Mongodb Features
//! 
//! The following mongodb functions are not implemented, because I couldn't find a good way to return the modified document after an update with sqlite in a single SQL statement.
//! * find_one_and_replace
//! * find_one_and_update
//! * find_and_modify
//! 
//! Aggregation is also not implemented, it is not a feature I use very much. I will look into it later.
//! 
//! Transaction implementation is also different from mongodb. Hoardbase's transaction can't return records. It is mainly used for creating related documents.
//! 
//! ## Internals
//! The key mechanism for storing and querying json data using sqlite is serializing json documents into the blob type. Currently [`bson`] is used 
//! as the serialized format. Another interesting format is [Amazon Ion](https://amzn.github.io/ion-docs/). I may add support for Ion in the future
//! when its rust binding matures. 
//! 
//! Indexing and searching is implemented using sqlite's [application-defined functions](https://www.sqlite.org/appfunc.html). Basically, we can define
//! custom functions that operates on the blob type to extract a json field, or patch a blob. As long as those custom functions are deterministic, they
//! can be used for indexing and searching. For example, we can define a function `bson_field(path, blob)` that extracts a bson field from the blob.
//! If we invoke this function with `WHERE bson_field('name.id', blob) = 3` against a collection, we will find all documents with name.id equals to 3. We can
//! also create indices on bson fields using this function. For more references, these are some good links:
//! 
//! [how to query json within a database](https://stackoverflow.com/questions/68447802/how-to-query-json-within-a-database)
//! 
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


#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_find() {

        std::fs::remove_file("test_find.db").unwrap_or(());
    
        {
            println!("{:?}", env!("CARGO_PKG_VERSION"));
            let mut config = database::DatabaseConfig::new("test_find.db");
            config.trace(true);
            config.profile(true);
        
            let mut db = database::Database::open(&config).unwrap();

            let mut ccol: base::CollectionConfig = base::CollectionConfig::default("test_collect");
            ccol.hash_document(true);
            ccol.log_last_modified(true);
            
            let mut collection = db.create_collection("test_collect", &ccol).unwrap();
            collection.create_index(&json!({"age": 1}), false).unwrap();

            collection.insert_one(&json!({ "kind": "apples", "qty": 5 })).unwrap();
            collection.insert_one(&json!({ "kind": "bananas", "qty": 7 })).unwrap();
            collection.insert_one(&json!({ "kind": "oranges", "qty": { "in stock": 8, "ordered": 12 } })).unwrap();
            collection.insert_one(&json!({ "kind": "avocados", "qty": "fourteen" })).unwrap();

            let row = collection.find_one(&json!({ "kind": "apples" }), 0).unwrap();

            assert_eq!(row.data.as_object().unwrap().get("kind").unwrap().as_str().unwrap(), "apples");
            assert_eq!(row.data.as_object().unwrap().get("qty").unwrap().as_i64().unwrap(), 5);

           
        }
    
        std::fs::remove_file("test_find.db").unwrap();


    }

}


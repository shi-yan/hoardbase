# Hoardbase

[![crates.io](https://img.shields.io/crates/v/hoardbase.svg)](https://crates.io/crates/hoardbase) [![docs.rs](https://img.shields.io/docsrs/hoardbase)](https://docs.rs/hoardbase/0.1.0-alpha/hoardbase)
 
Hoardbase is sqlite disguised as a NoSql database with an API similar to that of mongodb. There had been many times that I need a single-file embedded NoSql solution and couldn't find any. For my use cases, a good choice should meet the following requirements:

1. It needs to be NoSql. This is convinent when data are dirty, which is common in the data ETL use case. Another benefit enabled by NoSql is less effort in implementing data backward compatibility. Even when a data schema can eventually be defined and a Sql database is desired, prototyping using NoSql is also easier.
2. The database has to be embeddable for easy deployment. In many use cases, for example, a standalone desktop app, end users might not have the skills for setting up and maintaining a database server.
3. The database must be contained in a single file. This will guarantee data integrity and make data migration and backup easier for untrained users. 
4. There should be cross-language support (at least for C/C++, Python, Rust and Nodejs). Although there have been projects that share the same goals above spiritually, such as [tingodb](https://github.com/sergeyksv/tingodb), [mongita](https://github.com/scottrogowski/mongita) and [litedb](https://www.litedb.org/), their language choice affects portability.
5. It is desired to have an API similar to that of mongodb. Not only because mongodb has a large user base, but Mongodb's API is also akin to json, which is also often used for communication. It is more intuitive to translate a json request into a db query when the two are similar.

I feel that an embeddable NoSql is a very common building block that lacks good choices. The cloest one, in my opinion, is [ejdb2](https://ejdb.org/). However, that project is inactive and its code readability is poor. But what about this project? Sqlite is a solid fundation and has been battle tested. I try to keep my warpper layer simple and its internal well documented to make sure fixability. 

## Installation

### Rust
```toml
[dependencies]
hoardbase = "0.1.0-alpha"
```

## GUI Admin

## Build

python binding, see [here](https://pyo3.rs) for more.
```bash
cd python
python3 -m venv .env
source .env/bin/activate
pip3 install maturin
maturin develop
python3 test.py
```

cpp binding
```bash
cargo install cbindgen --force
cd cpp
mkdir build
cd build
cmake ..
make
./hoardbase_test
```

nodejs binding, see [here](https://neon-bindings.com/docs/hello-world) for more.
```bash
cd nodejs
npm i --save
npm run build
npm run test
```

hoardmin, the TUI admin tool.
```bash
cd hoardmin
Cargo run --bin hoardmin
```

<!-- cargo-sync-readme start -->

## Usage
Hoardbase tries to provide a similar programming interface as that of mongodb. If you are already familiar with mongodb, using Hoardbase should be 
very simple.

### Opening a database, Creating a collection, and inserting a record

Rust:
```rust
use hoardbase::database::{DatabaseConfig, Database};
use hoardbase::base::{CollectionConfig};
use crate::hoardbase::base::CollectionTrait;
use serde_json::json;

fn main() {
    let mut config = DatabaseConfig::new("test.db");
    config.trace(true);
    config.profile(true);
    let mut db = Database::open(&config).unwrap();
    let mut ccol: CollectionConfig = CollectionConfig::default("test");
    ccol.hash_document(true);
    ccol.log_last_modified(true);
    let mut collection = db.create_collection("test", &ccol).unwrap();
    collection.create_index(bson::to_bson(&json!({"age": 1})).unwrap().as_document().unwrap(), false).unwrap();
    collection.insert_one( bson::to_bson(&json!({ "kind": "apples", "qty": 5 })).unwrap().as_document().unwrap()).unwrap();
}
```

Python:
```python
import hoardbase
db = hoardbase.Database.open('test.db')
col = db.create_collection('test')
r = col.insert_one({'name': 'test'})
```

Nodejs:
```javascript
const Database = require('hoardbase')
let db = new Database(path)
let col = db.createCollection("test")
let r = col.insertOne({ data: "test", age: 23, test_arr: [1, 2, 3], test_obj: { a: 1, b: 2 } })
```

## Unsupported Mongodb Features

The following mongodb functions are not implemented, because I couldn't find a good way to return the modified document after an update with sqlite in a single SQL statement.
* find_one_and_replace
* find_one_and_update
* find_and_modify

Aggregation is also not implemented, it is not a feature I use very much. I will look into it later.

Transaction implementation is also different from mongodb. Hoardbase's transaction can't return records. It is mainly used for creating related documents.

## Internals
The key mechanism for storing and querying json data using sqlite is serializing json documents into the blob type. Currently [`bson`] is used 
as the serialized format. Another interesting format is [Amazon Ion](https://amzn.github.io/ion-docs/). I may add support for Ion in the future
when its rust binding matures. 

Indexing and searching is implemented using sqlite's [application-defined functions](https://www.sqlite.org/appfunc.html). Basically, we can define
custom functions that operates on the blob type to extract a json field, or patch a blob. As long as those custom functions are deterministic, they
can be used for indexing and searching. For example, we can define a function `bson_field(path, blob)` that extracts a bson field from the blob.
If we invoke this function with `WHERE bson_field('name.id', blob) = 3` against a collection, we will find all documents with name.id equals to 3. We can
also create indices on bson fields using this function. For more references, these are some good links:

[how to query json within a database](https://stackoverflow.com/questions/68447802/how-to-query-json-within-a-database)

[sqlite json support](https://dgl.cx/2020/06/sqlite-json-support)

<!-- cargo-sync-readme end -->
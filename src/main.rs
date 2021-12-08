use std::fs::File;
use std::io::Write;
use serde_json::json;
#[macro_use] extern  crate  slugify;

mod database;
mod collection;
mod query_translator;

fn main() {
    println!("Hello, world!");

    let mut db = database::Database::open("debug.db").unwrap();

    {
        let mut collection =  db.create_collection("test_collect.wef").unwrap();
        collection.create_index(&json!({"age": 1}), false).unwrap();
    }

    db.collection("test_collect.wef").unwrap().find();

    let collections = db.list_collections();

    for coll in collections {
        println!("{}", coll.borrow().name);
    }
    
    for age in 18..300 {
        db.collection("test_collect.wef").unwrap().insert_one(&json!(
            {
                "name": format!("test{}", age),
                "age": age,
                "test_struct": {
                    "name": "test",
                    "age": 10
                }
            }
        )).unwrap();
    }

    let result = db.collection("test_collect.wef").unwrap().find_one(&json!(
        {
            "age": 299
        }
    )).unwrap();

    println!("{:?}", result);

    let query1 = json!(
        { "size": { "h": 14, "w": 21, "uom": "cm" } }
    );

    let translator = query_translator::QueryTranslator{};

    let mut params = Vec::<rusqlite::types::Value>::new();

    println!("{} ({:?})", translator.query_document(&query1, &mut params).unwrap(), params);

    let query2 = json!(
        { "size.uom": "in" }
    );
    params.clear();
    println!("{} ({:?})", translator.query_document(&query2, &mut params).unwrap(), params);

    let query3 = json!(
        { "size.h": { "$lt": 14 } }
    );
    params.clear();
    println!("{} ({:?})", translator.query_document(&query3, &mut params).unwrap(), params);

    let query4 = json!(
        { "size.h": { "$lt": 15 }, "size.uom": "in", "status": "D" }
    );
    params.clear();
    println!("{} ({:?})", translator.query_document(&query4, &mut params).unwrap(), params);

    let query5 = json!(
        { "$nor": [ {"a": 15}, {"b": 32}, {"c" : 40} ], "size.uom": "in", "status": "D", "name": "test" }
    );
    params.clear();
    println!("{} ({:?})", translator.query_document(&query5, &mut params).unwrap(), params);

    let count = db.collection("test_collect.wef").unwrap().count_document(&json!({})).unwrap();
    println!("document count {}", count);
}

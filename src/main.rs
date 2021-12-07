use std::fs::File;
use std::io::Write;
use serde_json::json;

mod database;
mod collection;
mod query_translator;

fn main() {
    println!("Hello, world!");

    let mut db = database::Database::open("debug.db").unwrap();
    //conn.init();
    {
        let mut collection =  db.create_collection("test_collect.wef").unwrap();
        collection.find();
    }

    db.collection("test_collect.wef").unwrap().find();

    let collections = db.list_collections();

    for coll in collections {
        println!("{}", coll.borrow().name);
    }
        
    db.collection("test_collect.wef").unwrap().insert_one(json!(
        {
            "name": "test",
            "age": 10,
            "test_struct": {
                "name": "test",
                "age": 10
            }
        }
    )).unwrap();

    let result = db.collection("test_collect.wef").unwrap().find_one(json!(
        {
            "age": 10
        }
    )).unwrap();

    println!("{:?}", result);

    let query1 = json!(
        { "size": { "h": 14, "w": 21, "uom": "cm" } }
    );

    let translator = query_translator::QueryTranslator{};

    println!("{}", translator.query_document(&query1).unwrap());

    let query2 = json!(
        { "size.uom": "in" }
    );

    println!("{}", translator.query_document(&query2).unwrap());

    let query3 = json!(
        { "size.h": { "$lt": 14 } }
    );

    println!("{}", translator.query_document(&query3).unwrap());

    let query4 = json!(
        { "size.h": { "$lt": 15 }, "size.uom": "in", "status": "D" }
    );

    println!("{}", translator.query_document(&query4).unwrap());

    let query5 = json!(
        { "$nor": { "a": 15, "b": 32, "c" : 40 }, "size.uom": "in", "status": "D", "name": "test" }
    );

    println!("{}", translator.query_document(&query5).unwrap());
}

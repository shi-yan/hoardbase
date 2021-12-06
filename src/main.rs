use std::fs::File;
use std::io::Write;
use serde_json::json;

mod database;
mod collection;

fn main() {
    println!("Hello, world!");

    let mut db = database::Database::open("debug.db").unwrap();
    //conn.init();
    let collection =  db.create_collection("test_collect.wef").unwrap();

    collection.borrow_mut().find();

    db.collection("test_collect.wef").unwrap().borrow_mut().find();

    let collections = db.list_collections();

    for coll in collections {
        println!("{}", coll.borrow().name);
    }
        
    db.collection("test_collect.wef").unwrap().borrow_mut().insert_one(json!(
        {
            "name": "test",
            "age": 10
        }
    )).unwrap();

    let result = db.collection("test_collect.wef").unwrap().borrow_mut().find_one(json!(
        {
            "_id": 1
        }
    )).unwrap();

    println!("{:?}", result);
}

use serde_json::json;
use std::fs::File;
use std::io::Write;
#[macro_use]
extern crate slugify;

mod collection;
mod database;
mod query_translator;

use collection::SearchOption;

fn main() {
    println!("Hello, world!");

    let mut config = database::DatabaseConfig::new("test.db");
    config.trace(true);
    config.profile(true);

    let mut db = database::Database::open(&config).unwrap();

    {
        let mut ccol: database::CollectionConfig = database::CollectionConfig::default();
        ccol.hash_document(true);
        ccol.log_last_modified(true);
        
        let mut collection = db.create_collection("test_collect.wef", &ccol).unwrap();
        collection.create_index(&json!({"age": 1}), false).unwrap();
    }

    //db.collection("test_collect.wef").unwrap().find();

    let collections = db.list_collections();

    for coll in collections {
        println!("{}", coll.borrow().get_name());
    }
    for age in 18..300 {
        db.collection("test_collect.wef")
            .unwrap()
            .insert_one(&json!(
                {
                    "name": format!("test{}", age),
                    "age": age,
                    "test_struct": {
                        "name": "test",
                        "age": 10
                    }
                }
            ))
            .unwrap();
    }

    let result = db
        .collection("test_collect.wef")
        .unwrap()
        .find_one(
            &json!(
                {
                    "age": 299
                }
            ),
            0,
        )
        .unwrap();

    println!("{:?}", result);

    let query1 = json!(
        { "size": { "h": 14, "w": 21, "uom": "cm" } }
    );

    let translator = query_translator::QueryTranslator {};

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

    let count = db.collection("test_collect.wef").unwrap().count_document(&json!({}), &None).unwrap();
    println!("document count {}", count);

    let distinct_count = db.collection("test_collect.wef").unwrap().distinct("age", &None, &None).unwrap();

    println!("distinct count {}", distinct_count);

    let indices = db.collection("test_collect.wef").unwrap().get_indexes().unwrap();

    println!("indices {:?}", indices);

    let delete_one_result = db
        .collection("test_collect.wef")
        .unwrap()
        .delete_one(&json!(
            {
                "age": 299
            }
        ))
        .unwrap();

    println!("delete one result {:?}", delete_one_result);

   /* match db.collection("test_collect.wef").unwrap().find_one(
        &json!(
            {
                "_id": delete_one_result.unwrap().id
            }
        ),
        0,
    ) {
        Ok(r) => {
            println!("verify deletion result {:?}", r);
        }
        Err(e) => {
            println!("verify deletion error {:?}", e);
        }
    };*/
    let mut records = Vec::<collection::Record>::new();
    let r = &mut records; //&mut move |record| -> std::result::Result<(), &'static str>
    db.collection("test_collect.wef").unwrap().find(&json!({"age":279}), search_option!(10),  process_record!( record => {
        println!("call back {:?}", record);
        r.push(record.clone());
        Ok(())
    })).unwrap();

    println!("records {:?}", records);
}

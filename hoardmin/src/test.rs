use quick_js::{Context, JsValue, Callback};
use std::collections::hash_map::HashMap;
use serde_json::json;
use hoardbase::database::{DatabaseConfig, Database};
use hoardbase::base::CollectionTrait;
fn main() {
    let context = Context::new().unwrap();
    context.add_callback("myCallback", |a: i32, b: i32| a + b).unwrap();

    let mut config = DatabaseConfig::new("test_find.db");
    config.trace(true);
    config.profile(true);

    let mut db = Database::open(&config).unwrap();

    context.add_callback("insertOne", move |collection_name: String, doc: HashMap<String, JsValue>| -> JsValue {
        println!("insert one callback: {}, {:?}", collection_name, doc);
        db.collection(&collection_name).unwrap().insert_one(&json!({"test": 32}));
        let mut vec_res = Vec::<JsValue>::new();
        vec_res.push(quick_js::JsValue::Object( doc));
        quick_js::JsValue::Array(vec_res)
    }).unwrap();

    context.eval(&format!("
    class Collection {{
        constructor(name){{
            this.name = name;
        }}
        insertOne(doc) {{
            return insertOne(this.name, doc)
        }}
    }}
    class Db{{
        constructor(){{
            this.test_collection = new Collection('test_collection')
        }}
    }}
    var db = new Db();
    ")).unwrap();



    
    let value = context.eval("db.test_collection.insertOne({age: 33, val: 'test'})").unwrap();
    println!("{:?}", value);
   // println!("{:?}", vec_res);
    //let value2 = context.eval("kk").unwrap();
    //println!("{:?}", value2);
}
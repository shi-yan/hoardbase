use neon::prelude::*;
use serde_json::Map;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::convert::TryInto;

fn hello(mut cx: FunctionContext) -> JsResult<JsString> {
    Ok(cx.string("hello node"))
}

struct Database {
    pub db: Arc<Mutex<hoardbase::database::Database>>,
}

struct Collection {
    name: String,
    db: Arc<Mutex<hoardbase::database::Database>>,
}

impl Finalize for Database {}

impl Finalize for Collection {}

impl Database {

    // Create a new instance of `Database` and place it inside a `JsBox`
    // JavaScript can hold a reference to a `JsBox`, but the contents are opaque
    fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<Database>> {

        let path = cx.argument::<JsString>(0)?.value(&mut cx);
        let mut db_config = hoardbase::database::DatabaseConfig::new(&path);
        db_config.trace(true);
        db_config.profile(true);
        let db = hoardbase::database::Database::open(&db_config).unwrap();

        let result = Database {
            db: Arc::new(Mutex::new(db)),
        };

        Ok(cx.boxed(result))
    }

    fn js_create_collection(mut cx: FunctionContext) -> JsResult<JsBox<Collection>> {
        let collection_name = cx.argument::<JsString>(0)?.value(&mut cx);
        println!("collection name: {}", collection_name);
        let db = cx.this().downcast_or_throw::<JsBox<Database>, _>(&mut cx)?;
        let mut ccol = hoardbase::base::CollectionConfig::default(&collection_name);
        ccol.hash_document(true);
        ccol.log_last_modified(true);
        let mut x = db.db.lock().unwrap();
        let r = x.create_collection(&collection_name, &ccol);
        match r {
            Ok(collection) => Ok(cx.boxed(Collection { name: collection_name.to_string(), db: db.db.clone() })),
            Err(e) => cx.throw_error(format!("{}", e)),
        }
    }
}

impl Collection {
    fn js_insert_one(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let collection = cx.this().downcast_or_throw::<JsBox<Collection>, _>(&mut cx)?;
        let obj = cx.argument::<JsObject>(0)?;
        let properties = obj.get_own_property_names(&mut cx).unwrap();
        let name_arr = properties.to_vec(&mut cx).unwrap();

        for name in name_arr {
            let name_str:String = name.to_string(&mut cx).unwrap().value(&mut cx);
            let val = obj.get(&mut cx, name_str.as_str()).unwrap();
            if val.is::<Handle<JsString>>() {
                println!("It's a string!");
            } else {
                println!("Not a string...");
            }
            println!("field {}", name_str);
        }

        Ok(cx.undefined())
    }
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
  //  cx.export_function("hello", hello)?;
    cx.export_function("databaseNew", Database::js_new)?;
    cx.export_function("databaseCreateCollection", Database::js_create_collection)?;
    cx.export_function("collectionInsertOne", Collection::js_insert_one)?;
    Ok(())
}

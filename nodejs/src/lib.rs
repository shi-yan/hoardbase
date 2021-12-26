use neon::prelude::*;
use serde_json::Map;
use std::cell::RefCell;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use hoardbase::base::CollectionTrait;

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

        let result = Database { db: Arc::new(Mutex::new(db)) };

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
    fn convert_js_obj2serde_json_map(cx: &mut FunctionContext, obj: &Handle<'_, JsObject>, output: &mut serde_json::Map<String, serde_json::Value>) {
        let properties = obj.get_own_property_names(cx).unwrap();
        let properties = properties.downcast_or_throw::<JsArray, _>(cx).unwrap();
        let property_vals = properties.to_vec(cx).unwrap();
        for name in property_vals {
            let inner_name = name.downcast_or_throw::<JsString, _>(cx).unwrap();
            let name_str: String = inner_name.value(cx);
            println!("name: {}", name_str);
            let val = obj.get(cx, name_str.as_str()).unwrap();

            if val.is_a::<JsString, _>(cx) {
                output.insert(name_str, serde_json::Value::from(val.downcast_or_throw::<JsString, _>(cx).unwrap().value(cx)));
            } else if val.is_a::<JsNumber, _>(cx) {
                output.insert(name_str, serde_json::Value::from(val.downcast_or_throw::<JsNumber, _>(cx).unwrap().value(cx)));
            } else if val.is_a::<JsBoolean, _>(cx) {
                output.insert(name_str, serde_json::Value::from(val.downcast_or_throw::<JsBoolean, _>(cx).unwrap().value(cx)));
            } else if val.is_a::<JsNull, _>(cx) {
                output.insert(name_str, serde_json::Value::Null);
            } else if val.is_a::<JsArray, _>(cx) {
                //array has to come before object, otherwise it will be treated as object
                let mut json = Vec::<serde_json::Value>::new();
                let inner_arr = val.downcast_or_throw::<JsArray, _>(cx).unwrap();
                Collection::convert_js_arr2serde_json_array(cx, &inner_arr, &mut json);
                output.insert(name_str, serde_json::Value::Array(json));
            } else if val.is_a::<JsObject, _>(cx) {
                let mut json = serde_json::Map::new();
                let inner_obj = val.downcast_or_throw::<JsObject, _>(cx).unwrap();
                Collection::convert_js_obj2serde_json_map(cx, &inner_obj, &mut json);
                output.insert(name_str, serde_json::Value::Object(json));
            } else {
                println!("Unrecognized field type: {}", name_str);
            }
        }
    }

    fn convert_js_arr2serde_json_array(cx: &mut FunctionContext, arr: &Handle<'_, JsArray>, output: &mut Vec<serde_json::Value>) {
        let vals = arr.downcast_or_throw::<JsArray, _>(cx).unwrap();
        let vals = vals.to_vec(cx).unwrap();
        for val in vals {
            if val.is_a::<JsString, _>(cx) {
                output.push(serde_json::Value::from(val.downcast_or_throw::<JsString, _>(cx).unwrap().value(cx)));
            } else if val.is_a::<JsNumber, _>(cx) {
                output.push(serde_json::Value::from(val.downcast_or_throw::<JsNumber, _>(cx).unwrap().value(cx)));
            } else if val.is_a::<JsBoolean, _>(cx) {
                output.push(serde_json::Value::from(val.downcast_or_throw::<JsBoolean, _>(cx).unwrap().value(cx)));
            } else if val.is_a::<JsNull, _>(cx) {
                output.push(serde_json::Value::Null);
            } else if val.is_a::<JsArray, _>(cx) {
                //array has to come before object, otherwise it will be treated as object
                let mut json = Vec::<serde_json::Value>::new();
                let inner_arr = val.downcast_or_throw::<JsArray, _>(cx).unwrap();
                Collection::convert_js_arr2serde_json_array(cx, &inner_arr, &mut json);
                output.push(serde_json::Value::Array(json));
            } else if val.is_a::<JsObject, _>(cx) {
                let mut json = serde_json::Map::new();
                let inner_obj = val.downcast_or_throw::<JsObject, _>(cx).unwrap();
                Collection::convert_js_obj2serde_json_map(cx, &inner_obj, &mut json);
                output.push(serde_json::Value::Object(json));
            } else {
                println!("Unrecognized field type");
            }
        }
    }

    fn js_insert_one(mut cx: FunctionContext) -> JsResult<JsUndefined> {
        let collection = cx.this().downcast_or_throw::<JsBox<Collection>, _>(&mut cx)?;
        let obj = cx.argument::<JsObject>(0)?;
        let mut json = serde_json::Map::new();
        Self::convert_js_obj2serde_json_map(&mut cx, &obj, &mut json);

        println!("{:?}", json);

        let result = collection.db.lock().unwrap().collection(collection.name.as_str()).unwrap().insert_one( &serde_json::Value::from(json)).unwrap();
        println!("{:?}", result);
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

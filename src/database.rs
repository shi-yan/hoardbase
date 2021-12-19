use crate::base::*;
use crate::collection::Collection;
use crate::transaction::TransactionCollection;
use bson::Bson;
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::rc::Weak;
use chrono::prelude::*;

enum UpdateOperator {
    Set,
    Unset,
    Inc,
    Min,
    Max,
    CurrentDate,
    Mul,
    Rename,
    SetOnInsert,
}

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub path: String,
    pub should_trace: bool,
    pub should_profile: bool,
}

impl DatabaseConfig {
    pub fn new(path: &str) -> Self {
        DatabaseConfig { path: String::from(path), should_trace: false, should_profile: false }
    }

    pub fn trace<'a>(&'a mut self, arg: bool) -> &'a mut DatabaseConfig {
        self.should_trace = arg;
        self
    }

    pub fn profile<'a>(&'a mut self, args: bool) -> &'a mut DatabaseConfig {
        self.should_profile = args;
        self
    }
}

#[derive(Debug)]
struct UserFunctionError {
    message: String,
}

impl fmt::Display for UserFunctionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Error for UserFunctionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

pub struct Database {
    config: DatabaseConfig,
    internal: rusqlite::Connection,
    collections: HashMap<String, (String, CollectionConfig)>,
}

pub struct Transaction<'conn> {
    connection: rusqlite::Transaction<'conn>,
    collections: HashMap<String, (String, CollectionConfig)>,
    //  collections: HashMap<String, std::rc::Rc<std::cell::RefCell<dyn CollectionTrait>>>,
}

impl<'a> Transaction<'a> {
    pub fn collection(&'a self, collection_name: &str) -> Result<TransactionCollection<'a>, &str> {
        if self.collections.contains_key(collection_name) {
            let (collection_name, collection_config) = self.collections.get(collection_name).unwrap();
            Ok(TransactionCollection::<'a> {
                config: collection_config.clone(),
                name: collection_name.clone(),
                db: &self.connection,
                table_name: collection_name.clone(),
            })
        } else {
            Err("No collection found")
        }
    }
}

#[macro_export]
macro_rules! process_record {
    // `()` indicates that the macro takes no argument.
    ($record: ident=> $body: block) => {
        // The macro will expand into the contents of this block.
        &mut move |$record| -> std::result::Result<(), &'static str> { $body }
    };
}

fn recursive_process(search_doc: &mut bson::Bson, split: &mut std::str::Split<&str>, operator: &UpdateOperator, value: &bson::Bson) -> Result<bool, String> {
    if let Some(part) = split.next() {
        if let Some(inner_doc) = search_doc.as_document() {
            if !inner_doc.contains_key(part) {
                search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Document(bson::Document::new()));
            }
        } else {
            search_doc.as_document_mut().unwrap().remove(part.to_string());
            search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Document(bson::Document::new()));
        }

        if let Some(bson_doc) = search_doc.as_document_mut().unwrap().get_mut(part) {
            if let Ok(r) = recursive_process(bson_doc, split, operator, value) {
                if r {
                    match operator {
                        UpdateOperator::Set => {
                            search_doc.as_document_mut().unwrap().remove(part);
                            search_doc.as_document_mut().unwrap().insert(part.to_string(), value);
                            return Ok(false);
                        }

                        UpdateOperator::Inc => {
                            let original_data = search_doc.as_document().unwrap().get(part).unwrap();
                            if original_data.element_type() == bson::spec::ElementType::Double {  
                                let d1: f64 = original_data.as_f64().unwrap();
                                if let bson::Bson::Double(d2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double(d1 + d2));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(i1 + i2));
                                    return Ok(false);
                                }
                            } else {
                                return Err("incorrect data type for operator inc".to_string());
                            }
                        }

                        UpdateOperator::Min => {
                            let original_data = search_doc.as_document().unwrap().get(part).unwrap();
                            if original_data.element_type() == bson::spec::ElementType::Double {  
                                let d1: f64 = original_data.as_f64().unwrap();
                                if let bson::Bson::Double(d2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double( d1.min(*d2)));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(  i1.min(*i2)));
                                    return Ok(false);
                                }
                            } else {
                                return Err("incorrect data type for operator min".to_string());
                            }
                        }

                        UpdateOperator::Max => {
                            let original_data = search_doc.as_document().unwrap().get(part).unwrap();
                            if original_data.element_type() == bson::spec::ElementType::Double {  
                                let d1: f64 = original_data.as_f64().unwrap();
                                if let bson::Bson::Double(d2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double( d1.max(*d2)));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(  i1.max(*i2)));
                                    return Ok(false);
                                }
                            } else {
                                return Err("incorrect data type for operator max".to_string());
                            }
                        }

                        UpdateOperator::Mul => {
                            let original_data = search_doc.as_document().unwrap().get(part).unwrap();
                            if original_data.element_type() == bson::spec::ElementType::Double {  
                                let d1: f64 = original_data.as_f64().unwrap();
                                if let bson::Bson::Double(d2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double( d1 *d2));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(  i1 *i2));
                                    return Ok(false);
                                }
                            } else {
                                return Err("incorrect data type for operator max".to_string());
                            }
                        }


                        UpdateOperator::CurrentDate => {
                            
                            if value.element_type() == bson::spec::ElementType::String {
                                let date_type = value.as_str().unwrap();
                                if date_type == "date" || date_type == "timestamp" {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    let utc: DateTime<Utc> = Utc::now(); 
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::DateTime::from(utc));
                                    return Ok(false);
                                }
                                else {
                                    return Err("incorrect date type for operator CurrentDate".to_string());
                                }
                            } else {
                                return Err("incorrect data type for operator CurrentDate".to_string());
                            }
                        }

                        _ => {}
                    }
                } else {
                    return Ok(false);
                }
            }
        }
        return Ok(false);
    } else {
        return Ok(true);
    }
}

impl Database {
    pub fn open<'b>(config: &DatabaseConfig) -> std::result::Result<Database, &str> {
        let mut connection = Database {
            config: config.clone(),
            internal: rusqlite::Connection::open(config.path.clone()).unwrap(),
            collections: HashMap::new(),
        };
        connection.init();
        Ok(connection)
    }

    pub fn path(&self) -> Option<String> {
        Some(self.config.path.clone())
    }

    fn init<'b>(&'b mut self) {
        if self.config.should_trace {
            self.internal.trace(Some(|statement| {
                println!("trace: {}", statement);
            }));
        }

        if self.config.should_profile {
            self.internal.profile(Some(|statement, duration| {
                println!("profile: {} {} nanos", statement, duration.as_nanos());
            }));
        }

        self.internal
            .create_scalar_function("json_field", 2, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
                assert_eq!(ctx.len(), 2, "called with unexpected number of arguments");

                let field_name = ctx.get_raw(0).as_str().unwrap();
                let blob = ctx.get_raw(1).as_blob().unwrap();

                let mut doc: bson::Bson = bson::from_reader(blob).unwrap();

                let split = field_name.split(".");

                for part in split {
                    if let bson::Bson::Document(inner_doc) = doc {
                        if let Some(bson_doc) = inner_doc.get(part) {
                            doc = bson_doc.clone();
                        } else {
                            return Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null)));
                        }
                    } else {
                        return Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null)));
                    }
                }

                match doc {
                    bson::Bson::Double(f) => Ok(Some(rusqlite::types::Value::from(f))),
                    bson::Bson::String(string) => Ok(Some(rusqlite::types::Value::from(string.clone()))),
                    bson::Bson::Array(_array) => Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null))),
                    bson::Bson::Document(_doc) => Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null))),
                    bson::Bson::Boolean(boolean) => Ok(Some(rusqlite::types::Value::from(boolean))),
                    bson::Bson::Null => Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null))),
                    bson::Bson::RegularExpression(regex) => Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null))),
                    bson::Bson::Int32(i) => Ok(Some(rusqlite::types::Value::from(i))),
                    bson::Bson::Int64(i) => Ok(Some(rusqlite::types::Value::from(i))),
                    bson::Bson::Timestamp(t) => {
                        let mut integer: i64 = t.increment.into();
                        integer <<= 32;
                        let time: i64 = t.time.into();
                        integer += time;
                        Ok(Some(rusqlite::types::Value::from(integer)))
                    }
                    bson::Bson::Binary(t) => Ok(Some(rusqlite::types::Value::from(t.bytes.clone()))),
                    bson::Bson::ObjectId(id) => Ok(Some(rusqlite::types::Value::from(id.to_hex()))),
                    bson::Bson::DateTime(dt) => Ok(Some(rusqlite::types::Value::from(dt.timestamp_millis()))),
                    bson::Bson::Decimal128(d) => Ok(Some(rusqlite::types::Value::from(Vec::from(d.bytes().clone())))),
                    _ => Ok(Some(rusqlite::types::Value::from(rusqlite::types::Null))),
                }
            })
            .unwrap();

        self.internal
            .create_scalar_function("sha1", 1, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
                assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");

                let blob = ctx.get_raw(0).as_blob().unwrap();
                let mut hasher = Sha1::new();
                hasher.update(blob);
                let result = hasher.finalize();
                let hex_string = hex::encode(result.as_slice());
                Ok(Some(hex_string))
            })
            .unwrap();

        self.internal
            .create_scalar_function("json_patch", 2, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
                let mut original_doc: bson::Bson = bson::Bson::Document(bson::Document::new());

                if ctx.get_raw(0) == rusqlite::types::ValueRef::Null {
                    let original_blob = ctx.get_raw(0).as_blob().unwrap();
                    original_doc = bson::from_reader(original_blob).unwrap();
                }

                let update_blob = ctx.get_raw(1).as_blob().unwrap();

                let update_doc: bson::Document = bson::from_reader(update_blob).unwrap();
                //https://docs.mongodb.com/manual/reference/operator/update/#std-label-update-operators
                for (key, value) in update_doc.iter() {
                   let operation:UpdateOperator =  match key.as_str() {
                        "$currentDate" => {
                            UpdateOperator::CurrentDate
                        }
                        "$inc" => {
                            UpdateOperator::Inc
                        }
                        "$min" => {
                            UpdateOperator::Min
                        }
                        "$max" => {
                            UpdateOperator::Max
                        }
                        "$mul" => {
                            UpdateOperator::Mul
                        }
                        "$rename" => {
                            UpdateOperator::Rename
                        }
                        "$set" => {
                            UpdateOperator::Set
                        }
                        "$setOnInsert" => {
                            UpdateOperator::SetOnInsert
                        }
                        "$unset" => {
                            UpdateOperator::Unset
                        }
                        _ => {
                            return Err(rusqlite::Error::UserFunctionError(Box::new(UserFunctionError { message: "unknown update operator".to_string() })));
                        }
                    };

                    if let bson::Bson::Document(doc) = value {
                        for (key2, new_value) in doc.iter() {
                            let mut split = key2.split(".");
                            if let Err(e) = recursive_process(&mut original_doc, &mut split, &operation, &new_value) {
                                return Err(rusqlite::Error::UserFunctionError(Box::new(UserFunctionError { message: e })));
                            }
                        }
                    }

                }

                let bson_doc = bson::ser::to_document(&original_doc).unwrap();
                let mut bytes: Vec<u8> = Vec::new();
                bson_doc.to_writer(&mut bytes).unwrap();
                return Ok(Some(rusqlite::types::Value::from(bytes)));

                
            })
            .unwrap();

        let tx = self.internal.transaction().unwrap();
        {
            tx.execute(
                "CREATE TABLE IF NOT EXISTS _hoardbase (
                      id              INTEGER PRIMARY KEY,
                      collection      TEXT NOT NULL,
                      type            INTEGER NOT NULL,
                      table_name      TEXT UNIQUE NOT NULL,
                      hash_document   BOOLEAN NOT NULL,
                      log_last_modified BOOLEAN NOT NULL,
                      encrypt          BOOLEAN NOT NULL,
                      compress         BOOLEAN NOT NULL
                      )",
                [],
            )
            .unwrap();
            tx.execute(&format!("CREATE UNIQUE INDEX IF NOT EXISTS collection ON _hoardbase(collection);"), []).unwrap();
        }
        tx.commit().unwrap();

        let mut stmt = self.internal.prepare("SELECT * FROM _hoardbase WHERE type=0").unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Ok(row_result) = rows.next() {
            if let Some(row) = row_result {
                let collection: String = row.get(1).unwrap();
                let table_name: String = row.get(3).unwrap();
                let hash_document: bool = row.get(4).unwrap();
                let log_last_modified: bool = row.get(5).unwrap();
                let encrypt: bool = row.get(6).unwrap();
                let compress: bool = row.get(7).unwrap();

                let collection_config: CollectionConfig = CollectionConfig {
                    name: collection.clone(),
                    table_name: table_name,
                    should_hash_document: hash_document,
                    should_log_last_modified: log_last_modified,
                    should_hash_unique: false,
                };

                // println!("{} {}", collection, table_name);

                self.collections.insert(collection.to_string(), (collection.to_owned(), collection_config.to_owned()));
            } else {
                break;
            }
        }
    }

    pub fn create_collection<'a>(&'a mut self, collection_name: &str, config: &CollectionConfig) -> Result<Collection<'a>, &str> {
        if self.collections.contains_key(collection_name) {
            let (collection_name, collection_config) = self.collections.get(collection_name).unwrap();
            Ok(Collection::<'a> {
                config: collection_config.clone(),
                name: collection_name.clone(),
                db: &self.internal,
                table_name: collection_name.clone(),
            })
        } else {
            let tx = self.internal.transaction().unwrap();

            {
                tx.execute(
                    &format!(
                        "CREATE TABLE [{}] (
                          _id              INTEGER PRIMARY KEY,
                          raw             BLOB NOT NULL
                          {}
                          {}
                          )",
                        collection_name,
                        if config.should_hash_document { ", _hash NCHAR(40) GENERATED ALWAYS AS (sha1(raw)) STORED" } else { "" },
                        if config.should_log_last_modified { ", _last_modified DATETIME" } else { "" },
                    ),
                    [],
                )
                .unwrap();
                if config.should_hash_document {
                    tx.execute(&format!("CREATE {} INDEX IF NOT EXISTS _hash ON [{}](_hash);", if config.should_hash_unique { "UNIQUE" } else { "" }, collection_name), []).unwrap();
                }

                let mut stmt = tx
                    .prepare_cached(
                        "INSERT INTO _hoardbase (collection ,type, table_name,
                    hash_document,
                    log_last_modified,
                    encrypt,
                    compress) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(collection) DO NOTHING",
                    )
                    .unwrap();
                stmt.execute([
                    rusqlite::types::Value::Text(String::from(collection_name)),
                    rusqlite::types::Value::Integer(0),
                    rusqlite::types::Value::Text(String::from(collection_name)),
                    rusqlite::types::Value::from(config.should_hash_document),
                    rusqlite::types::Value::from(config.should_log_last_modified),
                    rusqlite::types::Value::from(false),
                    rusqlite::types::Value::from(false),
                ])
                .unwrap();
            }
            tx.commit().unwrap();

            self.collections.insert(collection_name.to_string(), (collection_name.to_owned(), config.to_owned()));

            Ok(Collection::<'a> {
                config: config.clone(),
                name: collection_name.to_string(),
                db: &self.internal,
                table_name: collection_name.to_string(),
            })
        }
    }

    pub fn collection<'a>(&'a mut self, collection_name: &str) -> Result<Collection<'a>, &str> {
        if self.collections.contains_key(collection_name) {
            let (collection_name, collection_config) = self.collections.get(collection_name).unwrap();
            Ok(Collection::<'a> {
                config: collection_config.clone(),
                name: collection_name.clone(),
                db: &self.internal,
                table_name: collection_name.clone(),
            })
        } else {
            Err("No collection found")
        }
    }

    pub fn list_collections(&self) -> Vec<(String, CollectionConfig)> {
        let mut collections = Vec::new();
        for collection in self.collections.values() {
            collections.push(collection.clone());
        }
        collections
    }

    pub fn drop_collection(&self) {}
    pub fn rename_collection(&self) {}

    pub fn transaction<'a, F>(&'a mut self, f: F) -> Result<(), &str>
    where
        F: FnOnce(&Transaction) -> Result<(), &'static str>,
    {
        {
            // let mut conn = self.internal;

            let t = self.internal.transaction().unwrap();
            let mut transaction = Transaction { connection: t, collections: HashMap::new() };
            //  let tx =  TransactionInternal::<'a>{ connection: t  };

            //  let tx_weak = std::rc::Rc::downgrade(&tx);

            for (key, value) in &self.collections {
                transaction.collections.insert(key.to_string(), (key.to_string(), value.1.clone()));
            }

            f(&transaction).unwrap();
            transaction.connection.commit().unwrap();
        }
        // tx.commit().unwrap();
        Err("Not implemented")
    }
}

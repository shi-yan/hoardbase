use crate::collection::Collection;
use bson::Bson;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use sha1::{Sha1, Digest};

#[derive(Clone, Debug)]
pub struct Config {
    pub path: String,
    pub should_trace: bool,
    pub should_profile: bool,
    pub should_hash_document: bool,
    pub should_log_last_modified: bool,
    pub should_hash_unique: bool,
}

impl Config {
    pub fn new(path: &str) -> Config {
        Config {
            path: String::from(path),
            should_trace: false,
            should_profile: false,
            should_hash_document: false,
            should_log_last_modified: false,
            should_hash_unique: false,
        }
    }

    pub fn trace<'a>(&'a mut self, arg: bool) -> &'a mut Config {
        self.should_trace = arg;
        self
    }

    pub fn profile<'a>(&'a mut self, args: bool) -> &'a mut Config {
        self.should_profile = args;
        self
    }

    pub fn hash_document<'a>(&'a mut self, args: bool) -> &'a mut Config {
        self.should_hash_document = args;
        self
    }

    pub fn log_last_modified<'a>(&'a mut self, args: bool) -> &'a mut Config {
        self.should_log_last_modified = args;
        self
    }

    pub fn hash_unique<'a>(&'a mut self, args: bool) -> &'a mut Config {
        self.should_hash_unique = args;
        self
    }
}

pub struct Database {
    config: Config,
    connection: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>,
    collections: HashMap<String, std::rc::Rc<std::cell::RefCell<Collection>>>,
}

impl Database {
    pub fn open(config: &Config) -> std::result::Result<Database, &str> {
        if let Ok(conn) = rusqlite::Connection::open(config.path.clone()) {
            let mut connection = Database {
                config: config.clone(),
                connection: std::rc::Rc::new(std::cell::RefCell::new(conn)),
                collections: HashMap::new(),
            };
            connection.init();
            Ok(connection)
        } else {
            Err("Unable to open db.")
        }
    }

    pub fn path(&self) -> Option<String> {
        let path_conn = self.connection.borrow();
        Some(path_conn.path().unwrap().to_string_lossy().into_owned())
    }

    fn init(&mut self) {
        let mut conn = self.connection.borrow_mut();

        if self.config.should_trace {
            conn.trace(Some(|statement| {
                println!("trace: {}", statement);
            }));
        }

        if self.config.should_profile {
            conn.profile(Some(|statement, duration| {
                println!("profile: {} {} nanos", statement, duration.as_nanos());
            }));
        }

        conn.create_scalar_function("json_field", 2, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
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


        conn.create_scalar_function("sha1", 1, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
            assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");

            let blob = ctx.get_raw(0).as_blob().unwrap();
            let mut hasher = Sha1::new();
            hasher.update(blob);
            let result = hasher.finalize();
            let hex_string = hex::encode(result.as_slice());
            Ok(Some(hex_string))
        })
        .unwrap();

        let tx = conn.transaction().unwrap();
        {
            tx.execute(
                "CREATE TABLE IF NOT EXISTS _hoardbase (
                      id              INTEGER PRIMARY KEY,
                      collection      TEXT NOT NULL,
                      type            INTEGER NOT NULL,
                      table_name      TEXT UNIQUE NOT NULL
                      )",
                [],
            )
            .unwrap();
            tx.execute(&format!("CREATE UNIQUE INDEX IF NOT EXISTS collection ON _hoardbase(collection);"), []).unwrap();
        }
        tx.commit().unwrap();

        let mut stmt = conn.prepare("SELECT * FROM _hoardbase WHERE type=0").unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Ok(row_result) = rows.next() {
            if let Some(row) = row_result {
                let collection: String = row.get(1).unwrap();
                let table_name: String = row.get(3).unwrap();

                println!("{} {}", collection, table_name);

                self.collections.insert(
                    collection.to_string(),
                    std::rc::Rc::new(std::cell::RefCell::new(Collection {
                        name: collection.to_string(),
                        connection: self.connection.clone(),
                        table_name: table_name.to_string(),
                    })),
                );
            } else {
                break;
            }
        }
    }

    pub fn create_collection(&mut self, collection_name: &str) -> Result<std::cell::RefMut<Collection>, &str> {
        if self.collections.contains_key(collection_name) {
            Ok(self.collections.get(collection_name).clone().unwrap().borrow_mut())
        } else {
            let mut conn = self.connection.borrow_mut();
            let tx = conn.transaction().unwrap();

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
                        if self.config.should_hash_document { ", _hash NCHAR(40) GENERATED ALWAYS AS (sha1(raw)) STORED" } else { "" },
                        if self.config.should_log_last_modified { ", _last_modified DATETIME" } else { "" },
                    ),
                    [],
                )
                .unwrap();
                if self.config.should_hash_document {
                    tx.execute(&format!("CREATE {} INDEX IF NOT EXISTS hash ON [{}](_hash);", if self.config.should_hash_unique { "UNIQUE" } else { "" }, collection_name), []).unwrap();
                }

                let mut stmt = tx.prepare_cached("INSERT INTO _hoardbase (collection ,type, table_name) VALUES (?1, ?2, ?3) ON CONFLICT(collection) DO NOTHING").unwrap();
                stmt.execute(&[collection_name, "0", collection_name]).unwrap();
            }
            tx.commit().unwrap();
            self.collections.insert(
                collection_name.to_string(),
                std::rc::Rc::new(std::cell::RefCell::new(Collection {
                    name: collection_name.to_string(),
                    connection: self.connection.clone(),
                    table_name: collection_name.to_string(),
                })),
            );
            Ok(self.collections.get(collection_name).clone().unwrap().borrow_mut())
        }
    }

    pub fn collection(&mut self, collection_name: &str) -> Result<std::cell::RefMut<Collection>, &str> {
        if self.collections.contains_key(collection_name) {
            Ok(self.collections.get(collection_name).clone().unwrap().borrow_mut())
        } else {
            Err("No collection found")
        }
    }

    pub fn list_collections(&self) -> Vec<&std::rc::Rc<std::cell::RefCell<Collection>>> {
        let mut collections = Vec::new();
        for collection in self.collections.values() {
            collections.push(collection);
        }
        collections
    }

    pub fn drop_collection(&self) {}
    pub fn rename_collection(&self) {}
}

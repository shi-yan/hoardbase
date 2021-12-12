use crate::collection::Collection;
use crate::collection::CollectionTrait;
use bson::Bson;
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    pub path: String,
    pub should_trace: bool,
    pub should_profile: bool,
}

impl DatabaseConfig {
    pub fn new(path: &str) -> Self {
        DatabaseConfig {
            path: String::from(path),
            should_trace: false,
            should_profile: false
        }
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


#[derive(Clone, Debug)]
pub struct CollectionConfig {
    pub should_hash_document: bool,
    pub should_log_last_modified: bool,
    pub should_hash_unique: bool,
}

impl CollectionConfig {
    pub fn default() -> CollectionConfig {
        CollectionConfig {
            should_hash_document: false,
            should_log_last_modified: false,
            should_hash_unique: false,
        }
    }

    pub fn hash_document<'a>(&'a mut self, args: bool) -> &'a mut CollectionConfig {
        self.should_hash_document = args;
        self
    }

    pub fn log_last_modified<'a>(&'a mut self, args: bool) -> &'a mut CollectionConfig {
        self.should_log_last_modified = args;
        self
    }

    pub fn hash_unique<'a>(&'a mut self, args: bool) -> &'a mut CollectionConfig {
        self.should_hash_unique = args;
        self
    }
}

pub struct TransactionItem {
    statement: String,
    args: Vec<rusqlite::types::Value>,
}

pub struct DatabaseInternal {
    pub transaction_buffer: Option<Vec<TransactionItem>>,
    pub connection: rusqlite::Connection,
}

pub struct Database {
    config: DatabaseConfig,
    internal: Rc<RefCell<DatabaseInternal>>,
    collections: HashMap<String, std::rc::Rc<std::cell::RefCell<dyn CollectionTrait>>>,
}

#[macro_export]
macro_rules! process_record {
    // `()` indicates that the macro takes no argument.
    ($record: ident=> $body: block) => {
        // The macro will expand into the contents of this block.
        &mut move |$record| -> std::result::Result<(), &'static str> { $body }
    };
}


#[inline(always)]
fn create_collection_by_config(config: &CollectionConfig, name: &str, internal: &std::rc::Weak<std::cell::RefCell<DatabaseInternal>>, table_name: &str) -> std::rc::Rc<RefCell<dyn CollectionTrait>> {
    match (config.should_hash_document, config.should_log_last_modified) {
        (true, true) => std::rc::Rc::new(RefCell::new(Collection::<true, true, false, false> {
            name: name.to_string(),
            db: internal.clone(),
            table_name: table_name.to_string(),
        })),

        (true, false) => std::rc::Rc::new(RefCell::new(Collection::<true, false, false, false> {
            name: name.to_string(),
            db: internal.clone(),
            table_name: table_name.to_string(),
        })),
        (false, false) => std::rc::Rc::new(RefCell::new(Collection::<false, false, false, false> {
            name: name.to_string(),
            db: internal.clone(),
            table_name: table_name.to_string(),
        })),
        (false, true) => std::rc::Rc::new(RefCell::new(Collection::<false, true, false, false> {
            name: name.to_string(),
            db: internal.clone(),
            table_name: table_name.to_string(),
        })),
    }
}

impl Database {
    pub fn open(config: &DatabaseConfig) -> std::result::Result<Database, &str> {
        if let Ok(conn) = rusqlite::Connection::open(config.path.clone()) {
            let mut connection = Database {
                config: config.clone(),
                internal: Rc::new(RefCell::new(DatabaseInternal {
                    transaction_buffer: None,
                    connection: conn,
                })),
                collections: HashMap::new(),
            };
            connection.init();
            Ok(connection)
        } else {
            Err("Unable to open db.")
        }
    }

    pub fn path(&self) -> Option<String> {
        Some(self.config.path.clone())
    }

    fn init(&mut self) {
        let mut conn = self.internal.borrow_mut();

        if self.config.should_trace {
            conn.connection.trace(Some(|statement| {
                println!("trace: {}", statement);
            }));
        }

        if self.config.should_profile {
            conn.connection.profile(Some(|statement, duration| {
                println!("profile: {} {} nanos", statement, duration.as_nanos());
            }));
        }

        conn.connection.create_scalar_function("json_field", 2, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
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

        conn.connection.create_scalar_function("sha1", 1, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
            assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");

            let blob = ctx.get_raw(0).as_blob().unwrap();
            let mut hasher = Sha1::new();
            hasher.update(blob);
            let result = hasher.finalize();
            let hex_string = hex::encode(result.as_slice());
            Ok(Some(hex_string))
        })
        .unwrap();

        let tx = conn.connection.transaction().unwrap();
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

        let mut stmt = conn.connection.prepare("SELECT * FROM _hoardbase WHERE type=0").unwrap();
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
                     should_hash_document: hash_document,
                     should_log_last_modified: log_last_modified,
                     should_hash_unique: false,
                };

                println!("{} {}", collection, table_name);

                self.collections.insert(collection.to_string(), create_collection_by_config(&collection_config, &collection, &Rc::downgrade(&self.internal), &table_name));
            } else {
                break;
            }
        }
    }

    pub fn create_collection(&mut self, collection_name: &str, config: &CollectionConfig) -> Result<std::cell::RefMut<dyn CollectionTrait>, &str> {
        if self.collections.contains_key(collection_name) {
            Ok(self.collections.get(collection_name).clone().unwrap().borrow_mut())
        } else {
            let mut conn = self.internal.borrow_mut();
            let tx = conn.connection.transaction().unwrap();

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

                let mut stmt = tx.prepare_cached("INSERT INTO _hoardbase (collection ,type, table_name,
                    hash_document,
                    log_last_modified,
                    encrypt,
                    compress) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(collection) DO NOTHING").unwrap();
                stmt.execute([rusqlite::types::Value::Text(String::from(collection_name)), 
                rusqlite::types::Value::Integer(0),
                rusqlite::types::Value::Text( String::from(collection_name)),
                rusqlite::types::Value::from( config.should_hash_document ),
                rusqlite::types::Value::from( config.should_log_last_modified), 
                rusqlite::types::Value::from(false), rusqlite::types::Value::from(false)  ]).unwrap();
            }
            tx.commit().unwrap();

            self.collections.insert(collection_name.to_string(), create_collection_by_config(&config, &collection_name, &Rc::downgrade(&self.internal), &collection_name));

            Ok(self.collections.get(collection_name).clone().unwrap().borrow_mut())
        }
    }

    pub fn collection(&mut self, collection_name: &str) -> Result<std::cell::RefMut<dyn CollectionTrait>, &str> {
        if self.collections.contains_key(collection_name) {
            Ok(self.collections.get(collection_name).clone().unwrap().borrow_mut())
        } else {
            Err("No collection found")
        }
    }

    pub fn list_collections(&self) -> Vec<&std::rc::Rc<std::cell::RefCell<dyn CollectionTrait>>> {
        let mut collections = Vec::new();
        for collection in self.collections.values() {
            collections.push(collection);
        }
        collections
    }

    pub fn drop_collection(&self) {}
    pub fn rename_collection(&self) {}
}

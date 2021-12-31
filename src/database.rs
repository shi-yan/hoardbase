use crate::base::*;
use crate::collection::Collection;
use crate::transaction::TransactionCollection;
use bson::Bson;
use chrono::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::rc::Weak;

/// This is the operations that can be performed on a bson document. These operations are corresponding to the mongodb operations found on this page.
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
    AddToSet,
    Pop,
    Pull,
    Push,
    PullAll,
    Bit,
}

/// This struct can config a database. This struct uses the builder pattern.
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// The filepath of the database.
    pub path: String,
    /// Setting this to true will enable the tracing function. All composed SQL statements will be printed to the console.
    pub should_trace: bool,
    /// Setting this to true will profile each SQL execution.
    pub should_profile: bool,
}

impl DatabaseConfig {
    /// Creates a new DatabaseConfig with the given path.
    pub fn new(path: &str) -> Self {
        DatabaseConfig { path: String::from(path), should_trace: false, should_profile: false }
    }
    /// Enables tracing.
    pub fn trace<'a>(&'a mut self, arg: bool) -> &'a mut DatabaseConfig {
        self.should_trace = arg;
        self
    }
    /// Enable profiling.
    pub fn profile<'a>(&'a mut self, args: bool) -> &'a mut DatabaseConfig {
        self.should_profile = args;
        self
    }
}

/// This struct represents a custom error that can be thrown from a user defined sqlite function.
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

/// The core struct that represents a database.
pub struct Database {
    /// The database config, including the database's filepath.
    config: DatabaseConfig,
    /// This is the underneath sqlite connection.
    internal: rusqlite::Connection,
    /// This is a hash table of the existing collections in the database. This hash table only contains collections' name and configurations. When a user wants to
    /// access a collection, we will construct a collection object dynamically. A collection object is a wrapper of the underlying sqlite connection, as
    /// well as the collection's configurations.
    ///
    /// The reason that we want to dynamically construct a collection object, instead of storing pre-constructed collection objects in this hash map, is
    /// that a collection object needs to reference to the underlying sqlite connection. Self reference [is not easy](https://arunanshub.hashnode.dev/self-referential-structs-in-rust) in Rust.
    collections: HashMap<String, (String, CollectionConfig)>,
}

/// If a user wants to execute multiple statements in a Transaction, she needs to obtain a Transaction object first. This object provides a similar interface
/// to that of a Database object. A user should be able to perform the same set operations on a Transaction object as a Database object.
pub struct Transaction<'conn> {
    /// The underlying sqlite transaction. This is created from the Database's internal sqlite connection, hence the lifetime.
    connection: rusqlite::Transaction<'conn>,
    /// This is similar to the collections field found in the [`Database`] struct.
    collections: HashMap<String, (String, CollectionConfig)>,
}

impl<'a> Transaction<'a> {
    /// Access a collection given its name.
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

/// This macro is for convenience. The purpose of this macro is to construct a callback function to process find results.
/// Because the callback's signature is very complex, we recommend using this macro.
#[macro_export]
macro_rules! process_record {
    // `()` indicates that the macro takes no argument.
    ($record: ident=> $body: block) => {
        // The macro will expand into the contents of this block.
        &mut move |$record| -> std::result::Result<(), &'static str> { $body }
    };
}

/// This function is called by the [`Collection::update_many()`] and [`Collection::update_one()`] functions. We use this function to recursively search for a json field by a path string. Then, based on the operator and value, we perform
/// different operations on the document.
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
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double(d1.min(*d2)));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(i1.min(*i2)));
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
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double(d1.max(*d2)));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(i1.max(*i2)));
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
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Double(d1 * d2));
                                    return Ok(false);
                                }
                            } else if original_data.element_type() == bson::spec::ElementType::Int64 {
                                let i1: i64 = original_data.as_i64().unwrap();
                                if let bson::Bson::Int64(i2) = value {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::Bson::Int64(i1 * i2));
                                    return Ok(false);
                                }
                            } else {
                                return Err("incorrect data type for operator max".to_string());
                            }
                        }

                        UpdateOperator::CurrentDate => {
                            if value.element_type() == bson::spec::ElementType::String {
                                let date_type = value.as_str().unwrap();
                                // todo timestamp is not implemented yet
                                if date_type == "date" || date_type == "timestamp" {
                                    search_doc.as_document_mut().unwrap().remove(part);
                                    let utc: DateTime<Utc> = Utc::now();
                                    search_doc.as_document_mut().unwrap().insert(part.to_string(), bson::DateTime::from(utc));
                                    return Ok(false);
                                } else {
                                    return Err("incorrect date type for operator CurrentDate".to_string());
                                }
                            } else {
                                return Err("incorrect data type for operator CurrentDate".to_string());
                            }
                        }

                        UpdateOperator::Unset => {
                            search_doc.as_document_mut().unwrap().remove(part);
                            return Ok(false);
                        }

                        UpdateOperator::Rename => {
                            if let bson::Bson::String(new_name) = value {
                                let original_bson: bson::Bson = search_doc.as_document().unwrap().get(part).unwrap().clone();
                                search_doc.as_document_mut().unwrap().remove(part);
                                search_doc.as_document_mut().unwrap().insert(new_name, original_bson);
                            } else {
                                return Err("incorrect data type for operator Rename".to_string());
                            }
                        }

                        UpdateOperator::Pop => {
                            if let bson::Bson::Int32(pos) = value {
                                if *pos == 1 {
                                    search_doc.as_array_mut().unwrap().pop();
                                } else if *pos == -1 {
                                    search_doc.as_array_mut().unwrap().remove(0);
                                } else {
                                    return Err("incorrect position for operator Pop".to_string());
                                }
                            } else {
                                return Err("incorrect data type for operator Pop".to_string());
                            }
                        }

                        UpdateOperator::Push => {
                            let original_data = search_doc.as_document().unwrap().get(part).unwrap().clone();

                            search_doc.as_array_mut().unwrap().push(original_data);
                        }
                        //https://docs.mongodb.com/manual/reference/operator/update-array/
                        // todo: implement PushAll/pull $in, $each $position etc.
                        // todo: implement bitwise operators $bit
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
    pub fn open(config: &DatabaseConfig) -> std::result::Result<Database, &str> {
        let mut connection = Database {
            config: config.clone(),
            internal: rusqlite::Connection::open(config.path.clone()).unwrap(),
            collections: HashMap::new(),
        };
        connection.init();
        Ok(connection)
    }

    /// Obtain the filepath of this database.
    pub fn path(&self) -> Option<String> {
        Some(self.config.path.clone())
    }

    /// This is an internal function to initialize an empty database. The initialization steps include:
    ///
    /// 1. Installing callbacks for tracing or profiling.
    ///
    /// 2. Installing application-defined functions that are used for extracting bson field or patching bson document.
    ///
    /// 3. Creating meta tables. There are two meta tables. One contains global info, such as database version. Another table
    /// contains the list of existing collections and their configurations.
    ///
    /// 4. Fetching exisiting collections from the collection meta table and populate the collection hashmap.
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
        // todo: need to change to bson_field
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
        
        // blake3 is chosen as the hash function because it appears to be faster than other choices.
        // however, this is not verified by the author.
        // https://crates.io/crates/blake3
        self.internal
            .create_scalar_function("blake3", 1, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
                assert_eq!(ctx.len(), 1, "called with unexpected number of arguments");

                let blob = ctx.get_raw(0).as_blob().unwrap();
                let mut hasher = blake3::Hasher::new();
                hasher.update(blob);
                let result = hasher.finalize();
                let hex_string = hex::encode(result.as_bytes());
                Ok(Some(hex_string))
            })
            .unwrap();
        // todo: need to change to bson_patch
        self.internal
            .create_scalar_function("json_patch", 2, rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, move |ctx| {
                let mut original_doc: bson::Bson = bson::Bson::Document(bson::Document::new());
                let mut is_insert = false;
                if ctx.get_raw(0) != rusqlite::types::ValueRef::Null {
                    let original_blob = ctx.get_raw(0).as_blob().unwrap();
                    original_doc = bson::from_reader(original_blob).unwrap();
                } else {
                    is_insert = true;
                }

                let update_blob = ctx.get_raw(1).as_blob().unwrap();

                let update_doc: bson::Document = bson::from_reader(update_blob).unwrap();
                //https://docs.mongodb.com/manual/reference/operator/update/#std-label-update-operators
                for (key, value) in update_doc.iter() {
                    let operation: UpdateOperator = match key.as_str() {
                        "$currentDate" => UpdateOperator::CurrentDate,
                        "$inc" => UpdateOperator::Inc,
                        "$min" => UpdateOperator::Min,
                        "$max" => UpdateOperator::Max,
                        "$mul" => UpdateOperator::Mul,
                        "$rename" => UpdateOperator::Rename,
                        "$set" => UpdateOperator::Set,
                        "$setOnInsert" => {
                            if is_insert {
                                UpdateOperator::Set
                            } else {
                                continue;
                            }
                        }
                        "$unset" => UpdateOperator::Unset,
                        "$addToSet" => UpdateOperator::AddToSet,
                        "$pop" => UpdateOperator::Pop,
                        "$pull" => UpdateOperator::Pull,
                        "$push" => UpdateOperator::Push,
                        "$pullAll" => UpdateOperator::PullAll,
                        "$bit" => UpdateOperator::Bit,
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
                      compress         BOOLEAN NOT NULL,
                      serialization_method         TEXT NOT NULL
                      )",
                [],
            )
            .unwrap();

            tx.execute(&format!("CREATE UNIQUE INDEX IF NOT EXISTS collection ON _hoardbase(collection);"), []).unwrap();

            tx.execute(
                "CREATE TABLE IF NOT EXISTS _hoardbase_meta (
                      id              INTEGER PRIMARY KEY,
                      version      TEXT NOT NULL,
                      git_hash             TEXT NOT NULL,
                      format_version      INTEGER NOT NULL,
                      build_time   DATETIME NOT NULL
                      )",
                [],
            )
            .unwrap();

            tx.execute(
                "INSERT INTO _hoardbase_meta (version ,git_hash, format_version,
            build_time) VALUES (?1, ?2, ?3, datetime('now') );",
                [rusqlite::types::Value::from(env!("CARGO_PKG_VERSION").to_string()), rusqlite::types::Value::from(env!("GIT_HASH").to_string()), rusqlite::types::Value::from(0)],
            )
            .unwrap();
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

    /// Create and return a collection given its config. The collection's properties ([`CollectionConfig::should_log_last_modified`], [`CollectionConfig::should_hash_document`]) can't be changed once created.
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
                        if config.should_hash_document { ", _hash NCHAR(40) GENERATED ALWAYS AS (blake3(raw)) STORED" } else { "" },
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
                    compress,
                    serialization_method) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'bson') ON CONFLICT(collection) DO NOTHING",
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

    /// Obtain an existing collection given a name. This function assemble a [`Collection`] object by combining
    /// the collection's configuration and the [`Database::internal`] rusqlite connection
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

    /// List existing collections
    pub fn list_collections(&self) -> Vec<(String, CollectionConfig)> {
        let mut collections = Vec::new();
        for collection in self.collections.values() {
            collections.push(collection.clone());
        }
        collections
    }

    /// Drop a collection
    pub fn drop_collection(&mut self, collection_name: &str) -> Result<(), &str> {
        if self.collections.contains_key(collection_name) {
            let tx = self.internal.transaction().unwrap();
            {
                tx.execute(&format!("DROP TABLE IF EXISTS [{}];", collection_name), []).unwrap();

                tx.execute(
                    &format!(
                        "DELETE FROM [{}]
                    WHERE collection = '{}';",
                        collection_name, collection_name
                    ),
                    [],
                )
                .unwrap();
            }
            tx.commit().unwrap();

            self.collections.remove(collection_name);
            return Ok(());
        }
        Err("No collection found")
    }

    /// Rename collection
    pub fn rename_collection(&mut self, collection_old_name: &str, collection_new_name: &str) -> Result<(), &str> {
        if self.collections.contains_key(collection_old_name) {
            let tx = self.internal.transaction().unwrap();
            {
                tx.execute(&format!("ALTER TABLE [{}] RENAME TO [{}];", collection_old_name, collection_new_name), []).unwrap();

                tx.execute(
                    &format!(
                        "UPDATE _hoardbase
                    SET collection = '{}', table_name = '{}'
                    WHERE collection = '{}';",
                         collection_new_name, collection_new_name, collection_old_name
                    ),
                    [],
                )
                .unwrap();
            }
            tx.commit().unwrap();
            let old_collection = self.collections.get(collection_old_name).unwrap().clone();
            self.collections.remove(collection_old_name);
            self.collections.insert(collection_new_name.to_string(), (collection_new_name.to_owned(), old_collection.1.to_owned()));
            return Ok(());
        }
        Err("No collection found")
    }

    /// Create a transaction
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

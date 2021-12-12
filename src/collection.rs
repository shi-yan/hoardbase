use bson::ser::Serializer;
use bson::Bson;
use bson::Document;
use chrono::prelude::*;
use rusqlite::params;
use rusqlite::params_from_iter;
use serde_json::json;
use serde_json::Value;
use slugify::slugify;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use crate::query_translator::QueryTranslator;
use crate::database::DatabaseInternal;

fn translate_index_config(config: &serde_json::Value, scope: &str, fields: &mut Vec<(String, i8)>) -> std::result::Result<(), &'static str> {
    if config.is_object() {
        for (key, value) in config.as_object().unwrap().iter() {
            if value.is_object() {
                return translate_index_config(&value, &format!("{}{}.", scope, key), fields);
            } else if value.is_number() {
                let order = value.as_i64().unwrap();

                if order != -1 && order != 1 {
                    return Err("Invalid order");
                }

                fields.push((format!("{}{}", scope, key), order as i8));
                return Ok(());
            } else {
                return Err("Invalid index config");
            }
        }
        Err("no members in index config")
    } else {
        Err("Index config must be an object")
    }
}

pub struct Collection<const H: bool, const L: bool, const E: bool, const C: bool> {
    pub name: String,
    pub db: Weak<RefCell<DatabaseInternal>>,
    pub table_name: String,
}

#[macro_export]
macro_rules! search_option {
    // The pattern for a single `eval`
    ($l:expr) => 
        {
            &Some(*SearchOption::default().limit($l))
        };

    // Decompose multiple `eval`s recursively
    ($l:expr, $s:expr) => {
        &Some(*SearchOption::default().limit($l).skip($s))
    };
}


#[derive(Debug, Clone, Copy)]
pub struct SearchOption {
    pub limit: i64,
    pub skip: i64,
}

impl SearchOption {
    pub fn default() -> Self {
        SearchOption { limit: -1, skip: 0 }
    }

    pub fn limit<'a>(&'a mut self, arg: i64) -> &'a mut SearchOption {
        self.limit = arg;
        self
    }

    pub fn skip<'a>(&'a mut self, args: i64) -> &'a mut SearchOption {
        self.skip = args;
        self
    }
}


#[derive(Debug, Clone)]
pub struct Record {
    pub id: i64,
    pub data: serde_json::Value,
    pub hash: String,
    pub last_modified: DateTime<Utc>,
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record {{ id: {}, data: {}, hash: {}, last_modified: {} }}", self.id, self.data, self.hash, self.last_modified)
    }
}

pub trait CollectionTrait {
    fn find(&mut self, query: &serde_json::Value, options: &Option<SearchOption>, f:&mut dyn FnMut(&Record) -> std::result::Result<(), &'static str> ) -> std::result::Result<(), &str> ;
    
    fn get_name(&self) -> &str;
    fn get_table_name(&self) -> &str;

    fn count_document(&mut self, query: &serde_json::Value, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;
    fn create_index(&mut self, config: &serde_json::Value, is_unique: bool) -> std::result::Result<(), String>;

    fn delete_one(&mut self, query: &serde_json::Value) -> std::result::Result<Option<Record>, String>;
    fn delete_many(&mut self);
    fn distinct(&mut self, field: &str, query: &Option<&serde_json::Value>, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;
    fn drop(&mut self);

    fn drop_index(&mut self);

    fn find_one(&mut self, query: &serde_json::Value, skip: i64) -> std::result::Result<Record, &str>;
    fn find_one_and_delete(&mut self);
    fn find_one_and_replace(&mut self);
    fn find_one_and_update(&mut self);
    fn find_and_modify(&mut self);
    fn get_indexes(&mut self) -> Result<Vec<serde_json::Value>, String>;

    fn insert_one(&mut self, document: &serde_json::Value) -> std::result::Result<(), String>;

    fn insert_many(&mut self);

    fn reindex(&mut self);
    fn replace_one(&mut self);

    fn update_one(&mut self);
    fn update_many(&mut self);
}

impl<const H: bool, const L: bool, const E: bool, const C: bool> CollectionTrait for Collection<H, L, E, C> {
    fn find(&mut self, query: &serde_json::Value, options: &Option<SearchOption>, f:&mut dyn FnMut(&Record) -> std::result::Result<(), &'static str> ) -> std::result::Result<(), &str> 
    {
        println!("call find for collection {}", self.name);

        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str : String = QueryTranslator {}.query_document(&query, &mut params).unwrap();

        let mut option_str = String::new();

        if let Some(opt) = options {
            option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
        }

        let db_internal = self.db.upgrade().unwrap();
        let conn = db_internal.borrow_mut();
        let mut stmt = conn.connection.prepare_cached(&format!("SELECT * FROM [{}] {} {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();

        let mut rows = stmt.query(params_from_iter(params.iter())).unwrap();

        while let Ok(row_result) = rows.next() {
            if let Some(row) = row_result {
                let id = row.get::<_, i64>(0).unwrap();
                let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
                let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
                let record = match (H, L) {
                    (false, false) => Record {
                        id: id,
                        data: json_doc,
                        hash: String::new(),
                        last_modified: Utc.timestamp(0, 0),
                    },
                    (true, false) => {
                        let hash = row.get::<_, String>(2).unwrap();
                        Record { id: id, data: json_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }
                    }
                    (true, true) => {
                        let hash = row.get::<_, String>(2).unwrap();
                        let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                        Record { id: id, data: json_doc, hash: hash, last_modified: last_modified }
                    }
                    (false, true) => {
                        let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                        Record {
                            id: id,
                            data: json_doc,
                            hash: String::new(),
                            last_modified: last_modified,
                        }
                    }
                };
                f(&record).unwrap();

            } else {
                break;
            }
        }

        Ok(())

    }

    fn count_document(&mut self, query: &serde_json::Value, options: &Option<SearchOption>) -> std::result::Result<i64, &str> {
        //todo implement skip limit
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let mut option_str = String::new();
        if let Some(opt) = options {
            option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
        }
        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();
        let mut stmt = conn.connection.prepare_cached(&format!("SELECT COUNT(1) FROM [{}] {} {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
        let count = stmt.query_row(params_from_iter(params.iter()), |row| Ok(row.get::<_, i64>(0).unwrap())).unwrap();
        Ok(count)
    }

    fn create_index(&mut self, config: &serde_json::Value, is_unique: bool) -> std::result::Result<(), String> {
        //todo implement type and size index
        let mut fields: Vec<(String, i8)> = Vec::new();

        let result = translate_index_config(&config, "", &mut fields);

        if let Err(e) = result {
            return Err(String::from(e));
        }
        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();
        let mut index_name = String::new();
        let mut config_str = String::new();
        for field in fields {
            if config_str.len() > 0 {
                config_str.push_str(",");
            }
            config_str.push_str(&format!("json_field('{}', raw) {}", field.0, if field.1 == 1 { "ASC" } else { "DESC" }));
            index_name.push_str(field.0.as_str());
            index_name.push_str("_");
        }

        index_name = slugify!(index_name.as_str(), separator = "_");

        match conn.connection.execute(&format!("CREATE {} INDEX IF NOT EXISTS {} ON [{}]({});", if is_unique { "UNIQUE" } else { "" }, index_name, &self.table_name, &config_str), []) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_table_name(&self) -> &str {
        self.table_name.as_str()
    }

    fn delete_one(&mut self, query: &serde_json::Value) -> std::result::Result<Option<Record>, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();
        // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT
        let mut stmt = conn.connection.prepare_cached(&format!("DELETE FROM [{}] WHERE _id = (SELECT _id FROM [{}] {} LIMIT 1) RETURNING *;", &self.table_name, &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();

        match stmt.query_row(params_from_iter(params.iter()), |row| {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            match (H, L) {
                (false, false) => Ok(Some(Record {
                    id: id,
                    data: json_doc,
                    hash: String::new(),
                    last_modified: Utc.timestamp(0, 0),
                })),
                (true, false) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    Ok(Some(Record { id: id, data: json_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }))
                }
                (true, true) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                    Ok(Some(Record { id: id, data: json_doc, hash: hash, last_modified: last_modified }))
                }
                (false, true) => {
                    let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                    Ok(Some(Record {
                        id: id,
                        data: json_doc,
                        hash: String::new(),
                        last_modified: last_modified,
                    }))
                }
            }
        }) {
            Ok(Some(record)) => Ok(Some(record)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn delete_many(&mut self) {}

    fn distinct(&mut self, field: &str, query: &Option<&serde_json::Value>, options: &Option<SearchOption>) -> std::result::Result<i64, &str> {
        //todo implement skip limit
        let mut params = Vec::<rusqlite::types::Value>::new();
        let mut where_str: String = String::new();
        if let Some(q) = query {
            where_str = QueryTranslator {}.query_document(q, &mut params).unwrap();
        }
        //println!("where_str {}", &where_str);
        let mut option_str = String::new();
        if let Some(opt) = options {
            option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
        }

        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();

        let mut stmt = conn.connection.prepare_cached(&format!("SELECT COUNT(DISTINCT json_field('{}', raw)) FROM [{}] {} {};", field, &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
        let count = stmt.query_row(params_from_iter(params.iter()), |row| Ok(row.get::<_, i64>(0).unwrap())).unwrap();
        Ok(count)
    }

    fn drop(&mut self) {}

    fn drop_index(&mut self) {}

    fn find_one(&mut self, query: &serde_json::Value, skip: i64) -> std::result::Result<Record, &str> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();
        let mut stmt = conn.connection
            .prepare_cached(&format!("SELECT * FROM [{}] {} LIMIT 1 {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }))
            .unwrap();

        if H == false && L == false {
            let row = stmt
                .query_row(params_from_iter(params.iter()), |row| {
                    Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap() /*, row.get::<_, String>(2).unwrap()*/))
                })
                .unwrap();

            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record {
                id: row.0,
                data: json_doc,
                hash: String::new(),
                last_modified: Utc.timestamp(0, 0),
            })
        } else if H == true && L == false {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, String>(2).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record {
                id: row.0,
                data: json_doc,
                hash: row.2,
                last_modified: Utc.timestamp(0, 0),
            })
        } else if H == true && L == true {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, String>(2).unwrap(), row.get::<_, DateTime<Utc>>(3).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record { id: row.0, data: json_doc, hash: row.2, last_modified: row.3 })
        } else if H == false && L == true {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, DateTime<Utc>>(2).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record { id: row.0, data: json_doc, hash: String::new(), last_modified: row.2 })
        } else {
            Err("Unable to find document")
        }
    }

    fn find_one_and_delete(&mut self) {}
    fn find_one_and_replace(&mut self) {}
    fn find_one_and_update(&mut self) {}
    fn find_and_modify(&mut self) {}
    fn get_indexes(&mut self) -> Result<Vec<serde_json::Value>, String> {
        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();

        println!("{}", format!("SELECT * FROM pragma_index_list('{}');", self.table_name));
        let mut stmt = conn.connection.prepare(&format!("SELECT * FROM pragma_index_list('{}');", self.table_name)).unwrap();
        let mut rows = stmt.query([]).unwrap();

        let mut result: Vec<serde_json::Value> = Vec::new();
        while let Ok(row_result) = rows.next() {
            if let Some(row) = row_result {
                result.push(json!({
                    "seq": row.get::<_, i64>(0).unwrap(),
                    "name": row.get::<_, String>(1).unwrap(),
                    "isUnique": row.get::<_, bool>(2).unwrap(),
                    "type": row.get::<_, String>(3).unwrap(),
                    "isPartial": row.get::<_, bool>(4).unwrap(),
                }));
            } else {
                break;
            }
        }
        Ok(result)
    }

    fn insert_one(&mut self, document: &serde_json::Value) -> std::result::Result<(), String> {
        let bson_doc = bson::ser::to_document(&document).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        bson_doc.to_writer(&mut bytes).unwrap();
        let db_internal = self.db.upgrade().unwrap();
        let mut conn = db_internal.borrow_mut();

        if L {
            let mut stmt = conn.connection.prepare_cached(&format!("INSERT INTO [{}] (raw, _last_modified) VALUES (?1, datetime('now'))", &self.table_name)).unwrap();
            let bytes_ref: &[u8] = bytes.as_ref();
            match stmt.execute(&[bytes_ref]) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.to_string());
                }
            }
        } else {
            let mut stmt = conn.connection.prepare_cached(&format!("INSERT INTO [{}] (raw) VALUES (?1)", &self.table_name)).unwrap();
            let bytes_ref: &[u8] = bytes.as_ref();
            match stmt.execute(&[bytes_ref]) {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    return Err(e.to_string());
                }
            }
        }
    }

    fn insert_many(&mut self) {}

    fn reindex(&mut self) {}
    fn replace_one(&mut self) {}

    fn update_one(&mut self) {}
    fn update_many(&mut self) {}
}

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
use std::convert::From;

use crate::base::*;
use crate::query_translator::QueryTranslator;

/// This function translate a json index descriptor into a SQL index descriptor
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

/// This struct represents a collection
pub struct Collection<'a> {
    /// The collection config
    pub config: CollectionConfig,
    /// Collection name
    pub name: String,
    /// This is the sqlite connection
    pub db: &'a rusqlite::Connection,
    /// The sqlite table name for this collection. Not necessarily the same as the collection name.
    pub table_name: String,
}

impl<'a> CollectionTrait for Collection<'a> {
    fn find(&mut self, query: &bson::Bson, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &str> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => find_internal::<_, _, true, true>(self.db, &self.config, query, options, f),
            (true, false) => find_internal::<_, _, true, false>(self.db, &self.config, query, options, f),
            (false, false) => find_internal::<_, _, false, false>(self.db, &self.config, query, options, f),
            (false, true) => find_internal::<_, _, false, true>(self.db, &self.config, query, options, f),
        }

        /* let db_internal = self.db;
        let conn = db_internal;
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        let mut option_str = String::new();
        if let Some(opt) = options {
            option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
        }
        let mut stmt = conn.prepare_cached(&format!("SELECT * FROM [{}] {} {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
        let mut rows = stmt.query(params_from_iter(params.iter())).unwrap();
        while let Ok(row_result) = rows.next() {
            if let Some(row) = row_result {
                let id = row.get::<_, i64>(0).unwrap();
                let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
                let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
                let record = match (self.config.should_hash_document, self.config.should_log_last_modified) {
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
        Ok(())*/
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
        let db_internal = self.db;
        let conn = db_internal;
        let mut stmt = conn.prepare_cached(&format!("SELECT COUNT(1) FROM [{}] {} {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
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
        let db_internal = self.db;
        let conn = db_internal;
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

        match conn.execute(&format!("CREATE {} INDEX IF NOT EXISTS {} ON [{}]({});", if is_unique { "UNIQUE" } else { "" }, index_name, &self.table_name, &config_str), []) {
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

    fn delete_one(&mut self, query: &serde_json::Value) -> std::result::Result<usize, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let db_internal = self.db;
        let conn = db_internal;
        // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT
        let mut stmt = conn.prepare_cached(&format!("DELETE FROM [{}] WHERE _id = (SELECT _id FROM [{}] {} LIMIT 1);", &self.table_name, &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();

        match stmt.execute(params_from_iter(params.iter())) {
            Ok(count) => Ok(count),
            Err(_) => Err("Error".to_string()),
        }
    }

    fn changes(&mut self) -> std::result::Result<i64, String> {
        let db_internal = self.db;
        let conn = db_internal;

        let mut stmt = conn.prepare_cached("SELECT changes();").unwrap();

        let rows = stmt
            .query_row([], |row| {
                let count = row.get::<_, i64>(0).unwrap();
                Ok(count)
            })
            .unwrap();

        Ok(rows)
    }

    fn delete_many(&mut self, query: &serde_json::Value) -> std::result::Result<usize, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let db_internal = self.db;
        let conn = db_internal;
        // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT

        let mut stmt = conn.prepare_cached(&format!("DELETE FROM [{}] {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();
        match stmt.execute(params_from_iter(params.iter())) {
            Ok(count) => Ok(count),
            Err(e) => Err(e.to_string()),
        }
    }

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

        let db_internal = self.db;
        let conn = db_internal;

        let mut stmt = conn.prepare_cached(&format!("SELECT COUNT(DISTINCT json_field('{}', raw)) FROM [{}] {} {};", field, &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
        let count = stmt.query_row(params_from_iter(params.iter()), |row| Ok(row.get::<_, i64>(0).unwrap())).unwrap();
        Ok(count)
    }

    fn drop_index(&mut self, index_name: &str) -> std::result::Result<(), String> {
        let db_internal = self.db;
        let conn = db_internal;

        match conn.execute(&format!("DROP INDEX IF EXISTS {} ;", index_name), []) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn find_one(&mut self, query: &serde_json::Value, skip: i64) -> std::result::Result<Record, &str> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let db_internal = self.db;
        let conn = db_internal;
        let mut stmt = conn
            .prepare_cached(&format!("SELECT * FROM [{}] {} LIMIT 1 {};", &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }))
            .unwrap();

        if self.config.should_hash_document == false && self.config.should_log_last_modified == false {
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
        } else if self.config.should_hash_document == true && self.config.should_log_last_modified == false {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, String>(2).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record {
                id: row.0,
                data: json_doc,
                hash: row.2,
                last_modified: Utc.timestamp(0, 0),
            })
        } else if self.config.should_hash_document == true && self.config.should_log_last_modified == true {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, String>(2).unwrap(), row.get::<_, DateTime<Utc>>(3).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record { id: row.0, data: json_doc, hash: row.2, last_modified: row.3 })
        } else if self.config.should_hash_document == false && self.config.should_log_last_modified == true {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, DateTime<Utc>>(2).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            Ok(Record { id: row.0, data: json_doc, hash: String::new(), last_modified: row.2 })
        } else {
            Err("Unable to find document")
        }
    }

    fn find_one_and_delete(&mut self, query: &serde_json::Value) -> std::result::Result<Option<Record>, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let db_internal = self.db;
        let conn = db_internal;
        // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT
        let mut stmt = conn.prepare_cached(&format!("DELETE FROM [{}] WHERE _id = (SELECT _id FROM [{}] {} LIMIT 1) RETURNING *;", &self.table_name, &self.table_name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();

        match stmt.query_row(params_from_iter(params.iter()), |row| {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            match (self.config.should_hash_document, self.config.should_log_last_modified) {
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

    fn get_indexes(&mut self) -> Result<Vec<serde_json::Value>, String> {
        let db_internal = self.db;
        let conn = db_internal;

        println!("{}", format!("SELECT * FROM pragma_index_list('{}');", self.table_name));
        let mut stmt = conn.prepare(&format!("SELECT * FROM pragma_index_list('{}');", self.table_name)).unwrap();
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

    fn insert_one(&mut self, document: &serde_json::Value) -> std::result::Result<Option<Record>, String> {
        let bson_doc = bson::ser::to_document(&document).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        bson_doc.to_writer(&mut bytes).unwrap();

        let mut stmt = self
            .db
            .prepare_cached(&format!("INSERT INTO [{}] (raw {}) VALUES (?1 {}) RETURNING *", &self.table_name, if self.config.should_log_last_modified { ", _last_modified" } else { "" }, if self.config.should_log_last_modified { ", datetime('now')" } else { "" }))
            .unwrap();
        let bytes_ref: &[u8] = bytes.as_ref();
        match stmt.query_row(&[bytes_ref], |row| {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            match (self.config.should_hash_document, self.config.should_log_last_modified) {
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

    fn insert_many(&mut self, documents: &Vec<serde_json::Value>) -> std::result::Result<(), String> {
        let mut stmt = self
            .db
            .prepare_cached(&format!("INSERT INTO [{}] (raw {}) VALUES (?1 {})", &self.table_name, if self.config.should_log_last_modified { ", _last_modified" } else { "" }, if self.config.should_log_last_modified { ", datetime('now')" } else { "" }))
            .unwrap();
        for doc in documents {
            let bson_doc = bson::ser::to_document(&doc).unwrap();
            let mut bytes: Vec<u8> = Vec::new();
            bson_doc.to_writer(&mut bytes).unwrap();

            let bytes_ref: &[u8] = bytes.as_ref();
            stmt.execute(&[bytes_ref]).unwrap();
        }

        Ok(())
    }

    fn reindex(&mut self) -> std::result::Result<(), String> {
        self.db.execute(&format!("REINDEX [{}]", &self.table_name), []).unwrap();

        Ok(())
    }

    fn replace_one(&mut self, query: &serde_json::Value, replacement: &serde_json::Value, skip: i64) -> std::result::Result<Option<Record>, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let bson_doc = bson::ser::to_document(&replacement).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        bson_doc.to_writer(&mut bytes).unwrap();
        params.push(rusqlite::types::Value::Blob(bytes));

        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();

        let mut stmt = self
            .db
            .prepare_cached(&format!(
                "UPDATE [{}] SET raw=?1 {} WHERE _id = (
                    SELECT
                        _id
                    FROM
                        [{}] 
                    {} LIMIT 1 {}
                ) RETURNING *;",
                &self.table_name,
                if self.config.should_log_last_modified { ", _last_modified = datetime('now')" } else { "" },
                &self.table_name,
                if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
                if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }
            ))
            .unwrap();

        match stmt.query_row(params_from_iter(params.iter()), |row| {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
            let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
            match (self.config.should_hash_document, self.config.should_log_last_modified) {
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

    fn update_one(&mut self, query: &serde_json::Value, update: &serde_json::Value, skip: i64, upsert: bool) -> std::result::Result<Option<Record>, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let update_bson_doc = bson::ser::to_document(&update).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        update_bson_doc.to_writer(&mut bytes).unwrap();
        params.push(rusqlite::types::Value::Blob(bytes));

        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();

        if upsert {
            let mut stmt = self
                .db
                .prepare_cached(&format!(
                    "INSERT INTO [{}] (_id, raw {}) VALUES ( (SELECT _id FROM [{}] {} LIMIT 1 {}) ,json_patch(NULL, ?1) {}) ON CONFLICT (_id) DO UPDATE SET raw=json_patch(raw,?1) {} RETURNING *;",
                    &self.table_name,
                    if self.config.should_log_last_modified { ", _last_modified" } else { "" },
                    &self.table_name,
                    if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
                    if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") },
                    if self.config.should_log_last_modified { ", datetime('now')" } else { "" },
                    if self.config.should_log_last_modified { ", _last_modified=datetime('now')" } else { "" }
                ))
                .unwrap();

            match stmt.query_row(params_from_iter(params.iter()), |row| {
                let id = row.get::<_, i64>(0).unwrap();
                let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
                let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
                match (self.config.should_hash_document, self.config.should_log_last_modified) {
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
        } else {
            let mut stmt = self
                .db
                .prepare_cached(&format!(
                    "UPDATE [{}] SET raw=json_patch(raw,?1) {} WHERE _id = (
                    SELECT
                        _id
                    FROM
                        [{}] 
                    {} LIMIT 1 {}
                ) RETURNING *;",
                    &self.table_name,
                    if self.config.should_log_last_modified { ", _last_modified=datetime('now')" } else { "" },
                    &self.table_name,
                    if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
                    if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }
                ))
                .unwrap();

            match stmt.query_row(params_from_iter(params.iter()), |row| {
                let id = row.get::<_, i64>(0).unwrap();
                let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
                let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
                match (self.config.should_hash_document, self.config.should_log_last_modified) {
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
    }

    /// This function update all documents match the `query` by the `update` object. If `upsert` is true, and no documents are found by
    /// query, we will create a new document using the `update` object.
    fn update_many(&mut self, query: &serde_json::Value, update: &serde_json::Value, limit: i64, skip: i64, upsert: bool) -> Result<i64, String> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let update_bson_doc = bson::ser::to_document(&update).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        update_bson_doc.to_writer(&mut bytes).unwrap();
        params.push(rusqlite::types::Value::Blob(bytes));

        let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();

        let mut stmt = self
            .db
            .prepare_cached(&format!(
                "SAVEPOINT updatemany; UPDATE [{}] SET raw=json_patch(raw,?1) {} 
                    {} {} {}; SELECT changes(); RELEASE SAVEPOINT updatemany;",
                &self.table_name,
                if self.config.should_log_last_modified { ", _last_modified=datetime('now')" } else { "" },
                if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
                if limit != 0 { format!("LIMIT {}", limit) } else { String::from("") },
                if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }
            ))
            .unwrap();

        match stmt.query_row(params_from_iter(params.iter()), |row| {
            let id = row.get::<_, i64>(0).unwrap();
            Ok(id)
        }) {
            Ok(record) => {
                if record == 0 && upsert {
                    let mut stmt = self
                        .db
                        .prepare_cached(&format!(
                            "INSERT INTO [{}] (raw {}) VALUES (json_patch(NULL, ?1) {}) RETURNING _id;",
                            &self.table_name,
                            if self.config.should_log_last_modified { ", _last_modified" } else { "" },
                            if self.config.should_log_last_modified { ", datetime('now')" } else { "" }
                        ))
                        .unwrap();

                    match stmt.query_row(params_from_iter(params.iter()), |row| {
                        let id = row.get::<_, i64>(0).unwrap();
                        Ok(id)
                    }) {
                        Ok(_) => Ok(1),
                        Err(e) => Err(e.to_string()),
                    }
                } else {
                    Ok(record)
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }
}

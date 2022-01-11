use bson::ser::Serializer;
use bson::Bson;
use bson::Document;
use chrono::prelude::*;
use rusqlite::params;
use rusqlite::params_from_iter;
use rusqlite::Params;
use serde_json::json;
use serde_json::Value;
use slugify::slugify;
use std::cell::RefCell;
use std::rc::Rc;
use std::rc::Weak;

use crate::query_translator::QueryTranslator;

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

#[macro_export]
macro_rules! search_option {
    ($l:expr) => {
        &Some(*SearchOption::default().limit($l))
    };

    ($l:expr, $s:expr) => {
        &Some(*SearchOption::default().limit($l).skip($s))
    };
}

#[derive(Clone, Debug)]
pub struct CollectionConfig {
    pub name: String,
    pub table_name: String,
    pub should_hash_document: bool,
    pub should_log_last_modified: bool,
    pub should_hash_unique: bool,
}

impl CollectionConfig {
    pub fn default(name: &str) -> CollectionConfig {
        CollectionConfig {
            name: name.to_string(),
            table_name: name.to_string(),
            should_hash_document: true,
            should_log_last_modified: true,
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

#[derive(Clone, Debug)]
pub struct Index {
    pub seq: i64,
    pub name: String,
    pub is_unique: bool,
    pub index_type: String,
    pub is_partial: bool,
}

impl std::fmt::Display for Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Index {{ seq: {}, name: {}, is_unique: {}, index_type: {} is_partial: {} }}", self.seq, self.name, self.is_unique, self.index_type, self.is_partial)
    }
}

#[derive(Debug, Clone)]
pub struct Record {
    pub id: i64,
    pub data: bson::Document,
    pub hash: String,
    pub last_modified: DateTime<Utc>,
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Record {{ id: {}, data: {}, hash: {}, last_modified: {} }}", self.id, self.data, self.hash, self.last_modified)
    }
}

pub trait CollectionTrait {
    fn find(&mut self, query: &bson::Document, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &str>;
    fn get_name(&self) -> &str;
    fn get_table_name(&self) -> &str;

    fn count_documents(&mut self, query: &bson::Document, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;
    fn create_index(&mut self, config: &bson::Document, is_unique: bool) -> std::result::Result<(), String>;

    fn delete_one(&mut self, query: &bson::Document) -> std::result::Result<usize, String>;
    fn changes(&mut self) -> std::result::Result<i64, String>;
    fn delete_many(&mut self, query: &bson::Document) -> std::result::Result<usize, String>;
    fn distinct(&mut self, field: &str, query: &Option<bson::Document>, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;

    fn drop_index(&mut self, index_name: &str) -> std::result::Result<(), String>;

    fn find_one(&mut self, query: &bson::Document, skip: i64) -> std::result::Result<Record, &str>;
    fn find_one_and_delete(&mut self, query: &bson::Document) -> std::result::Result<Option<Record>, String>;

    fn get_indexes(&mut self) -> Result<Vec<Index>, String>;

    fn insert_one(&mut self, document: &bson::Document) -> std::result::Result<Option<Record>, String>;

    fn insert_many(&mut self, documents: &Vec<bson::Document>) -> std::result::Result<(), String>;

    fn reindex(&mut self) -> std::result::Result<(), String>;
    fn replace_one(&mut self, query: &bson::Document, replacement: &bson::Document, skip: i64) -> std::result::Result<Option<Record>, String>;

    fn update_one(&mut self, query: &bson::Document, update: &bson::Document, skip: i64, upsert: bool) -> std::result::Result<Option<Record>, String>;

    fn update_many(&mut self, query: &bson::Document, update: &bson::Document, limit: i64, skip: i64, upsert: bool) -> Result<i64, String>;
}

pub trait Adapter<A> {
    fn prepare_cached_wrapper<'a>(&'a self, sql: &str) -> rusqlite::Result<rusqlite::CachedStatement<'a>>;
    fn execute_wrapper<'a, P: Params>(&'a self, sql: &str, params: P) -> rusqlite::Result<usize>;
    fn prepare_wrapper<'a>(&'a self, sql: &str) -> rusqlite::Result<rusqlite::Statement<'a>>;
}

impl Adapter<rusqlite::Connection> for rusqlite::Connection {
    fn prepare_cached_wrapper<'a>(&'a self, sql: &str) -> rusqlite::Result<rusqlite::CachedStatement<'a>> {
        self.prepare_cached(sql)
    }

    fn execute_wrapper<'a, P: Params>(&'a self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.execute(sql, params)
    }

    fn prepare_wrapper<'a>(&'a self, sql: &str) -> rusqlite::Result<rusqlite::Statement<'a>>{
        self.prepare(sql)
    }
}

impl<'conn> Adapter<rusqlite::Transaction<'conn>> for rusqlite::Transaction<'conn> {
    fn prepare_cached_wrapper<'a>(&'a self, sql: &str) -> rusqlite::Result<rusqlite::CachedStatement<'a>> {
        self.prepare_cached(sql)
    }

    fn execute_wrapper<'a, P: Params>(&'a self, sql: &str, params: P) -> rusqlite::Result<usize> {
        self.execute(sql, params)
    }

    fn prepare_wrapper<'a>(&'a self, sql: &str) -> rusqlite::Result<rusqlite::Statement<'a>>{
        self.prepare(sql)
    }
}

#[inline]
pub fn find_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, query: &bson::Document, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &'static str> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();

    let mut option_str = String::new();

    if let Some(opt) = options {
        option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
    }

    let mut stmt = conn.prepare_cached_wrapper(&format!("SELECT * FROM [{}] {} {};", &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();

    let mut rows = stmt.query(params_from_iter(params.iter())).unwrap();

    while let Ok(row_result) = rows.next() {
        if let Some(row) = row_result {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();

            let record = match (H, L) {
                (false, false) => Record {
                    id: id,
                    data: bson_doc,
                    hash: String::new(),
                    last_modified: Utc.timestamp(0, 0),
                },
                (true, false) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    Record { id: id, data: bson_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }
                }
                (true, true) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                    Record { id: id, data: bson_doc, hash: hash, last_modified: last_modified }
                }
                (false, true) => {
                    let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                    Record {
                        id: id,
                        data: bson_doc,
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

#[inline]
pub fn find_one_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, query: &bson::Document, skip: i64) -> std::result::Result<Record, &'static str> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();

    let mut stmt = conn
        .prepare_cached_wrapper(&format!("SELECT * FROM [{}] {} LIMIT 1 {};", &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }))
        .unwrap();

    match (H, L) {
        (false, false) => {
            let row = stmt
                .query_row(params_from_iter(params.iter()), |row| {
                    Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap() /*, row.get::<_, String>(2).unwrap()*/))
                })
                .unwrap();

            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();

            Ok(Record {
                id: row.0,
                data: bson_doc,
                hash: String::new(),
                last_modified: Utc.timestamp(0, 0),
            })
        }
        (true, false) => {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, String>(2).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();

            Ok(Record {
                id: row.0,
                data: bson_doc,
                hash: row.2,
                last_modified: Utc.timestamp(0, 0),
            })
        }
        (true, true) => {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, String>(2).unwrap(), row.get::<_, DateTime<Utc>>(3).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            Ok(Record { id: row.0, data: bson_doc, hash: row.2, last_modified: row.3 })
        }
        (false, true) => {
            let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap(), row.get::<_, DateTime<Utc>>(2).unwrap()))).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
            Ok(Record { id: row.0, data: bson_doc, hash: String::new(), last_modified: row.2 })
        }
    }
}

#[inline]
pub fn find_one_and_delete_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, query: &bson::Document) -> std::result::Result<Option<Record>, String> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();

    // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT
    let mut stmt = conn.prepare_cached_wrapper(&format!("DELETE FROM [{}] WHERE _id = (SELECT _id FROM [{}] {} LIMIT 1) RETURNING *;", &config.name, &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();

    match stmt.query_row(params_from_iter(params.iter()), |row| {
        let id = row.get::<_, i64>(0).unwrap();
        let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
        match (H, L) {
            (false, false) => Ok(Some(Record {
                id: id,
                data: bson_doc,
                hash: String::new(),
                last_modified: Utc.timestamp(0, 0),
            })),
            (true, false) => {
                let hash = row.get::<_, String>(2).unwrap();
                Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }))
            }
            (true, true) => {
                let hash = row.get::<_, String>(2).unwrap();
                let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: last_modified }))
            }
            (false, true) => {
                let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                Ok(Some(Record {
                    id: id,
                    data: bson_doc,
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

#[inline]
pub fn count_documents_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig, query: &bson::Document, options: &Option<SearchOption>) -> std::result::Result<i64, &'static str> {
    //todo implement skip limit
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();
    let mut option_str = String::new();
    if let Some(opt) = options {
        option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
    }

    let mut stmt = conn.prepare_cached_wrapper(&format!("SELECT COUNT(1) FROM [{}] {} {};", &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
    let count = stmt.query_row(params_from_iter(params.iter()), |row| Ok(row.get::<_, i64>(0).unwrap())).unwrap();
    Ok(count)
}

/// This function translate a json index descriptor into a SQL index descriptor
fn translate_index_config(config: &bson::Document, scope: &str, fields: &mut Vec<(String, i8)>) -> std::result::Result<(), &'static str> {
    for (key, value) in config.iter() {
        match value {
            bson::Bson::Document(doc) => {
                return translate_index_config(&doc, &format!("{}{}.", scope, key), fields);
            }
            bson::Bson::Int32(order) => {
                if *order != -1 && *order != 1 {
                    return Err("Invalid order");
                }

                fields.push((format!("{}{}", scope, key), *order as i8));
                return Ok(());
            }
            bson::Bson::Int64(order) => {
                if *order != -1 && *order != 1 {
                    return Err("Invalid order");
                }

                fields.push((format!("{}{}", scope, key), *order as i8));
                return Ok(());
            }
            _ => {
                return Err("Invalid index config");
            }
        }
    }
    Err("no members in index config")
}

#[inline]
pub fn create_index_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig, index_config: &bson::Document, is_unique: bool) -> std::result::Result<(), String> {
    //todo implement type and size index
    let mut fields: Vec<(String, i8)> = Vec::new();

    let result = translate_index_config(index_config, "", &mut fields);

    if let Err(e) = result {
        return Err(String::from(e));
    }

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

    match conn.execute_wrapper(&format!("CREATE {} INDEX IF NOT EXISTS {} ON [{}]({});", if is_unique { "UNIQUE" } else { "" }, index_name, &config.name, &config_str), []) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

#[inline]
pub fn delete_one_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig, query: &bson::Document) -> std::result::Result<usize, String> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();
    // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT
    let mut stmt = conn.prepare_cached_wrapper(&format!("DELETE FROM [{}] WHERE _id = (SELECT _id FROM [{}] {} LIMIT 1);", &config.name, &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();

    match stmt.execute(params_from_iter(params.iter())) {
        Ok(count) => Ok(count),
        Err(_) => Err("Error".to_string()),
    }
}

#[inline]
pub fn changes_internal<A, C: Adapter<A>>(conn: &C) -> std::result::Result<i64, String> {
    let mut stmt = conn.prepare_cached_wrapper("SELECT changes();").unwrap();

    let rows = stmt
        .query_row([], |row| {
            let count = row.get::<_, i64>(0).unwrap();
            Ok(count)
        })
        .unwrap();

    Ok(rows)
}

#[inline]
pub fn delete_many_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig, query: &bson::Document) -> std::result::Result<usize, String> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();

    // an alternative solution is SQLITE_ENABLE_UPDATE_DELETE_LIMIT

    let mut stmt = conn.prepare_cached_wrapper(&format!("DELETE FROM [{}] {};", &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") })).unwrap();
    match stmt.execute(params_from_iter(params.iter())) {
        Ok(count) => Ok(count),
        Err(e) => Err(e.to_string()),
    }
}

#[inline]
pub fn distinct_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig, field: &str, query: &Option<bson::Document>, options: &Option<SearchOption>) -> std::result::Result<i64, &'static str> {
    //todo implement skip limit
    let mut params = Vec::<rusqlite::types::Value>::new();
    let mut where_str: String = String::new();
    if let Some(q) = query {
        where_str = QueryTranslator {}.query_document(q, &mut params).unwrap();
    }
    let mut option_str = String::new();
    if let Some(opt) = options {
        option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
    }

    let mut stmt = conn.prepare_cached_wrapper(&format!("SELECT COUNT(DISTINCT json_field('{}', raw)) FROM [{}] {} {};", field, &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();
    let count = stmt.query_row(params_from_iter(params.iter()), |row| Ok(row.get::<_, i64>(0).unwrap())).unwrap();
    Ok(count)
}

#[inline]
pub fn drop_index_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig, index_name: &str) -> std::result::Result<(), String> {
    match conn.execute_wrapper(&format!("DROP INDEX IF EXISTS {} ;", index_name), []) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

#[inline]
pub fn get_indexes_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig) -> Result<Vec<Index>, String> {

    let mut stmt = conn.prepare_wrapper(&format!("SELECT * FROM pragma_index_list('{}');", config.name)).unwrap();
    let mut rows = stmt.query([]).unwrap();

    let mut result= Vec::new();
    while let Ok(row_result) = rows.next() {
        if let Some(row) = row_result {

            let index = Index {
                seq: row.get::<_, i64>(0).unwrap(),
                name: row.get::<_, String>(1).unwrap(),
                is_unique: row.get::<_, bool>(2).unwrap(),
                index_type: row.get::<_, String>(3).unwrap(),
                is_partial: row.get::<_, bool>(4).unwrap(),
            };

            result.push(index);
        } else {
            break;
        }
    }
    Ok(result)
}

#[inline]
pub fn insert_one_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, document: &bson::Document) -> std::result::Result<Option<Record>, String> {
    let bson_doc = bson::ser::to_document(&document).unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    bson_doc.to_writer(&mut bytes).unwrap();

    let mut stmt = conn
        .prepare_cached_wrapper(&format!("INSERT INTO [{}] (raw {}) VALUES (?1 {}) RETURNING *", &config.name, if L { ", _last_modified" } else { "" }, if L { ", datetime('now')" } else { "" }))
        .unwrap();
    let bytes_ref: &[u8] = bytes.as_ref();

    match stmt.query_row(&[bytes_ref], |row| {
        let id = row.get::<_, i64>(0).unwrap();
        let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();

        match (H, L) {
            (false, false) => Ok(Some(Record {
                id: id,
                data: bson_doc,
                hash: String::new(),
                last_modified: Utc.timestamp(0, 0),
            })),
            (true, false) => {
                let hash = row.get::<_, String>(2).unwrap();
                Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }))
            }
            (true, true) => {
                let hash = row.get::<_, String>(2).unwrap();
                let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: last_modified }))
            }
            (false, true) => {
                let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                Ok(Some(Record {
                    id: id,
                    data: bson_doc,
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

#[inline]
pub fn insert_many_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, documents: &Vec<bson::Document>) -> std::result::Result<(), String> {
    let mut stmt = conn
        .prepare_cached_wrapper(&format!("INSERT INTO [{}] (raw {}) VALUES (?1 {})", &config.name, if L { ", _last_modified" } else { "" }, if L { ", datetime('now')" } else { "" }))
        .unwrap();
    for doc in documents {
        let bson_doc = bson::ser::to_document(&doc).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        bson_doc.to_writer(&mut bytes).unwrap();

        let bytes_ref: &[u8] = bytes.as_ref();
        stmt.execute(&[bytes_ref]).unwrap();
    }
    // todo: handle error
    Ok(())
}

#[inline]
pub fn reindex_internal<A, C: Adapter<A>>(conn: &C, config: &CollectionConfig) -> std::result::Result<(), String> {
    // todo: handle error
    conn.execute_wrapper(&format!("REINDEX [{}]", &config.name), []).unwrap();

    Ok(())
}

#[inline]
pub fn replace_one_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, query: &bson::Document, replacement: &bson::Document, skip: i64) -> std::result::Result<Option<Record>, String> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let bson_doc = bson::ser::to_document(&replacement).unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    bson_doc.to_writer(&mut bytes).unwrap();
    params.push(rusqlite::types::Value::Blob(bytes));

    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();

    let mut stmt = conn
        .prepare_cached_wrapper(&format!(
            "UPDATE [{}] SET raw=?1 {} WHERE _id = (
                SELECT
                    _id
                FROM
                    [{}] 
                {} LIMIT 1 {}
            ) RETURNING *;",
            &config.name,
            if L { ", _last_modified = datetime('now')" } else { "" },
            &config.name,
            if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
            if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }
        ))
        .unwrap();

    match stmt.query_row(params_from_iter(params.iter()), |row| {
        let id = row.get::<_, i64>(0).unwrap();
        let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
        match (H, L) {
            (false, false) => Ok(Some(Record {
                id: id,
                data: bson_doc,
                hash: String::new(),
                last_modified: Utc.timestamp(0, 0),
            })),
            (true, false) => {
                let hash = row.get::<_, String>(2).unwrap();
                Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }))
            }
            (true, true) => {
                let hash = row.get::<_, String>(2).unwrap();
                let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: last_modified }))
            }
            (false, true) => {
                let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                Ok(Some(Record {
                    id: id,
                    data: bson_doc,
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

#[inline]
pub fn update_one_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig,  query: &bson::Document, update: &bson::Document, skip: i64, upsert: bool) -> std::result::Result<Option<Record>, String> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let update_bson_doc = bson::ser::to_document(&update).unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    update_bson_doc.to_writer(&mut bytes).unwrap();
    params.push(rusqlite::types::Value::Blob(bytes));

    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();

    if upsert {
        let mut stmt = conn
            .prepare_cached_wrapper(&format!(
                "INSERT INTO [{}] (_id, raw {}) VALUES ( (SELECT _id FROM [{}] {} LIMIT 1 {}) ,json_patch(NULL, ?1) {}) ON CONFLICT (_id) DO UPDATE SET raw=json_patch(raw,?1) {} RETURNING *;",
                &config.name,
                if L { ", _last_modified" } else { "" },
                &config.name,
                if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
                if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") },
                if L { ", datetime('now')" } else { "" },
                if L { ", _last_modified=datetime('now')" } else { "" }
            ))
            .unwrap();

        match stmt.query_row(params_from_iter(params.iter()), |row| {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
            match (H, L) {
                (false, false) => Ok(Some(Record {
                    id: id,
                    data: bson_doc,
                    hash: String::new(),
                    last_modified: Utc.timestamp(0, 0),
                })),
                (true, false) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }))
                }
                (true, true) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                    Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: last_modified }))
                }
                (false, true) => {
                    let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                    Ok(Some(Record {
                        id: id,
                        data: bson_doc,
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
        let mut stmt = conn
            .prepare_cached_wrapper(&format!(
                "UPDATE [{}] SET raw=json_patch(raw,?1) {} WHERE _id = (
                SELECT
                    _id
                FROM
                    [{}] 
                {} LIMIT 1 {}
            ) RETURNING *;",
                &config.name,
                if L { ", _last_modified=datetime('now')" } else { "" },
                &config.name,
                if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") },
                if skip != 0 { format!("OFFSET {}", skip) } else { String::from("") }
            ))
            .unwrap();

        match stmt.query_row(params_from_iter(params.iter()), |row| {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
            match (H, L) {
                (false, false) => Ok(Some(Record {
                    id: id,
                    data: bson_doc,
                    hash: String::new(),
                    last_modified: Utc.timestamp(0, 0),
                })),
                (true, false) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: Utc.timestamp(0, 0) }))
                }
                (true, true) => {
                    let hash = row.get::<_, String>(2).unwrap();
                    let last_modified = row.get::<_, DateTime<Utc>>(3).unwrap();
                    Ok(Some(Record { id: id, data: bson_doc, hash: hash, last_modified: last_modified }))
                }
                (false, true) => {
                    let last_modified = row.get::<_, DateTime<Utc>>(2).unwrap();
                    Ok(Some(Record {
                        id: id,
                        data: bson_doc,
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

#[inline]
pub fn update_many_internal<A, C: Adapter<A>, const H: bool, const L: bool>(conn: &C, config: &CollectionConfig, query: &bson::Document, update: &bson::Document, limit: i64, skip: i64, upsert: bool) -> Result<i64, String> {
    let mut params = Vec::<rusqlite::types::Value>::new();
    let update_bson_doc = bson::ser::to_document(update).unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    update_bson_doc.to_writer(&mut bytes).unwrap();
    params.push(rusqlite::types::Value::Blob(bytes));

    let where_str: String = QueryTranslator {}.query_document(&query, &mut params).unwrap();

    let mut stmt = conn
        .prepare_cached_wrapper(&format!(
            "SAVEPOINT updatemany; UPDATE [{}] SET raw=json_patch(raw,?1) {} 
                {} {} {}; SELECT changes(); RELEASE SAVEPOINT updatemany;",
            &config.name,
            if L { ", _last_modified=datetime('now')" } else { "" },
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
                let mut stmt = conn
                    .prepare_cached_wrapper(&format!(
                        "INSERT INTO [{}] (raw {}) VALUES (json_patch(NULL, ?1) {}) RETURNING _id;",
                        &config.name,
                        if L { ", _last_modified" } else { "" },
                        if L { ", datetime('now')" } else { "" }
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


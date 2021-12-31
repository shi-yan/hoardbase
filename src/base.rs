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
    pub index_type : String,
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

    fn count_document(&mut self, query: &bson::Document, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;
    fn create_index(&mut self, config: &bson::Document, is_unique: bool) -> std::result::Result<(), String>;

    fn delete_one(&mut self, query: &bson::Document) -> std::result::Result<usize, String>;
    fn changes(&mut self) -> std::result::Result<i64, String>;
    fn delete_many(&mut self, query: &bson::Document) -> std::result::Result<usize, String>;
    fn distinct(&mut self, field: &str, query: &Option<bson::Document>, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;

    fn drop_index(&mut self, index_name: &str) -> std::result::Result<(), String>;

    fn find_one(&mut self, query: &bson::Document, skip: i64) -> std::result::Result<Record, &str>;
    fn find_one_and_delete(&mut self, query: &bson::Document) -> std::result::Result<Option<Record>, String>;

    fn get_indexes(&mut self) ->  Result<Vec<Index>, String>;

    fn insert_one(&mut self, document: &bson::Document) -> std::result::Result<Option<Record>, String> ;

    fn insert_many(&mut self, documents: &Vec<bson::Document>) -> std::result::Result<(), String> ;

    fn reindex(&mut self) -> std::result::Result<(), String> ;
    fn replace_one(&mut self, query: &bson::Document, replacement: &bson::Document, skip: i64) -> std::result::Result<Option<Record>, String>;

    fn update_one(&mut self, query: &bson::Document, update: &bson::Document, skip: i64, upsert: bool) -> std::result::Result<Option<Record>, String> ;
    fn update_many(&mut self, query: &bson::Document, update: &bson::Document, limit: i64, skip: i64, upsert: bool) -> Result<i64, String>;
}

pub trait Adapter<A> {
    fn prepare_cached<'a>(&'a self, sql: &str) -> rusqlite:: Result<rusqlite:: CachedStatement<'a>> ;
}

impl Adapter<rusqlite::Connection> for rusqlite::Connection {
    fn prepare_cached<'a>(&'a self, sql: &str) -> rusqlite:: Result<rusqlite:: CachedStatement<'a>> {
        self.prepare_cached(sql)
    }
}

#[inline]
pub fn find_internal<A, C: Adapter<A>, const H:bool, const L:bool>(conn: &C, config: &CollectionConfig, query: &bson::Document, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &'static str>
{
    let mut params = Vec::<rusqlite::types::Value>::new();
    let where_str: String = QueryTranslator {}.query_document(query, &mut params).unwrap();

    let mut option_str = String::new();

    if let Some(opt) = options {
        option_str = format!("LIMIT {} OFFSET {}", opt.limit, opt.skip);
    }

    let mut stmt = conn.prepare_cached(&format!("SELECT * FROM [{}] {} {};", &config.name, if where_str.len() > 0 { format!("WHERE {}", &where_str) } else { String::from("") }, option_str)).unwrap();

    let mut rows = stmt.query(params_from_iter(params.iter())).unwrap();

    while let Ok(row_result) = rows.next() {
        if let Some(row) = row_result {
            let id = row.get::<_, i64>(0).unwrap();
            let bson_doc: bson::Document = bson::from_reader(row.get::<_, Vec<u8>>(1).unwrap().as_slice()).unwrap();
         
            let record = match (config.should_hash_document, config.should_log_last_modified) {
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
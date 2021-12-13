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

use crate::database::DatabaseInternal;
use crate::query_translator::QueryTranslator;
use fallible_streaming_iterator::FallibleStreamingIterator;

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
    fn find(&mut self, query: &serde_json::Value, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &str>;
    fn get_name(&self) -> &str;
    fn get_table_name(&self) -> &str;

    fn count_document(&mut self, query: &serde_json::Value, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;
    fn create_index(&mut self, config: &serde_json::Value, is_unique: bool) -> std::result::Result<(), String>;

    fn delete_one(&mut self, query: &serde_json::Value) -> std::result::Result<usize, String>;
    fn changes(&mut self) -> std::result::Result<i64, String>;
    fn delete_many(&mut self, query: &serde_json::Value) -> std::result::Result<usize, String>;
    fn distinct(&mut self, field: &str, query: &Option<&serde_json::Value>, options: &Option<SearchOption>) -> std::result::Result<i64, &str>;

    fn drop_index(&mut self, index_name: &str) -> std::result::Result<(), String>;

    fn find_one(&mut self, query: &serde_json::Value, skip: i64) -> std::result::Result<Record, &str>;
    fn find_one_and_delete(&mut self, query: &serde_json::Value) -> std::result::Result<Option<Record>, String>;
    fn find_one_and_replace(&mut self, query: &serde_json::Value, skip: i64);
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
/*
#[inline]
pub fn find_internal<C, const H:bool, const L:bool>(table_name: &str, conn: C, query: &serde_json::Value, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &'static str>
{


}*/
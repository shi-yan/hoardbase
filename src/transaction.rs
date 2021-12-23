
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

use crate::base::*;

pub struct TransactionCollection<'conn> {
    pub config: CollectionConfig,
    pub name: String,
    pub db: &'conn rusqlite::Transaction<'conn>,
    pub table_name: String,
}

impl<'conn> CollectionTrait for TransactionCollection<'conn> {
    fn find(&mut self, query: &serde_json::Value, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &str>{
        Err("afd")
    }
    fn get_name(&self) -> &str{
        return "test";
    }
    fn get_table_name(&self) -> &str{
        return "sf"
    }

    fn count_document(&mut self, query: &serde_json::Value, options: &Option<SearchOption>) -> std::result::Result<i64, &str>{
        Err("afd")
    }
    fn create_index(&mut self, config: &serde_json::Value, is_unique: bool) -> std::result::Result<(), String>{
        Err("afd".to_string())
    }

    fn delete_one(&mut self, query: &serde_json::Value) -> std::result::Result<usize, String>{
        Err("afd".to_string())
    }
    fn changes(&mut self) -> std::result::Result<i64, String>{
        Err("afd".to_string())
    }
    fn delete_many(&mut self, query: &serde_json::Value) -> std::result::Result<usize, String>{
        Err("afd".to_string())
    }
    fn distinct(&mut self, field: &str, query: &Option<&serde_json::Value>, options: &Option<SearchOption>) -> std::result::Result<i64, &str>{
        Err("afd")
    }

    fn drop_index(&mut self, index_name: &str) -> std::result::Result<(), String>{
        Err("afd".to_string())
    }

    fn find_one(&mut self, query: &serde_json::Value, skip: i64) -> std::result::Result<Record, &str>{
        Err("afd")
    }
    fn find_one_and_delete(&mut self, query: &serde_json::Value) -> std::result::Result<Option<Record>, String>{
        Err("afd".to_string())
    }
  //  fn find_one_and_replace(&mut self, query: &serde_json::Value, replacement: &serde_json::Value, skip: i64) -> std::result::Result<Record, String> {
  //      Err("ad".to_string())
  //  }
  //  fn find_one_and_update(&mut self){}
  //  fn find_and_modify(&mut self){}
    fn get_indexes(&mut self) -> Result<Vec<serde_json::Value>, String>{
        Err("afd".to_string())
    }

    fn insert_one(&mut self, document: &serde_json::Value) -> std::result::Result<Option<Record>, String>{
        Err("afd".to_string())
    }

    fn insert_many(&mut self, documents: &Vec<serde_json::Value>) -> std::result::Result<(), String> {
        Err("afd".to_string())
    }

    fn reindex(&mut self) -> std::result::Result<(), String> {
        Err("afd".to_string())
    }
    fn replace_one(&mut self, query: &serde_json::Value, replacement: &serde_json::Value, skip: i64) -> std::result::Result<Option<Record>, String>{
        Err("afd".to_string())
    }

    fn update_one(&mut self, query: &serde_json::Value, update: &serde_json::Value, skip: i64, upsert: bool) -> std::result::Result<Option<Record>, String>{
        Err("ada".to_string())
    }
    fn update_many(&mut self, query: &serde_json::Value, update: &serde_json::Value, limit: i64, skip: i64, upsert: bool) -> Result<i64, String>{
        Err("afd".to_string())
    }

}
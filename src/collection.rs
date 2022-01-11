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
use std::convert::From;
use std::rc::Rc;
use std::rc::Weak;

use crate::base::*;
use crate::query_translator::QueryTranslator;

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
    fn find(&mut self, query: &bson::Document, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &str> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => find_internal::<_, _, true, true>(self.db, &self.config, query, options, f),
            (true, false) => find_internal::<_, _, true, false>(self.db, &self.config, query, options, f),
            (false, false) => find_internal::<_, _, false, false>(self.db, &self.config, query, options, f),
            (false, true) => find_internal::<_, _, false, true>(self.db, &self.config, query, options, f),
        }
    }

    fn count_documents(&mut self, query: &bson::Document, options: &Option<SearchOption>) -> std::result::Result<i64, &str> {
        count_documents_internal(self.db, &self.config, query, options)
    }

    fn create_index(&mut self, config: &bson::Document, is_unique: bool) -> std::result::Result<(), String> {
        create_index_internal(self.db, &self.config, config, is_unique)
    }

    fn get_name(&self) -> &str {
        self.name.as_str()
    }

    fn get_table_name(&self) -> &str {
        self.table_name.as_str()
    }

    fn delete_one(&mut self, query: &bson::Document) -> std::result::Result<usize, String> {
        delete_one_internal(self.db, &self.config, query)
    }

    fn changes(&mut self) -> std::result::Result<i64, String> {
        changes_internal(self.db)
    }

    fn delete_many(&mut self, query: &bson::Document) -> std::result::Result<usize, String> {
        delete_many_internal(self.db, &self.config, query)
    }

    fn distinct(&mut self, field: &str, query: &Option<bson::Document>, options: &Option<SearchOption>) -> std::result::Result<i64, &str> {
        distinct_internal(self.db, &self.config, field, query, options)
    }

    fn drop_index(&mut self, index_name: &str) -> std::result::Result<(), String> {
        drop_index_internal(self.db, &self.config, index_name)
    }

    fn find_one(&mut self, query: &bson::Document, skip: i64) -> std::result::Result<Record, &str> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => find_one_internal::<_, _, true, true>(self.db, &self.config, query, skip),
            (true, false) => find_one_internal::<_, _, true, false>(self.db, &self.config, query, skip),
            (false, false) => find_one_internal::<_, _, false, false>(self.db, &self.config, query, skip),
            (false, true) => find_one_internal::<_, _, false, true>(self.db, &self.config, query, skip),
        }
    }

    fn find_one_and_delete(&mut self, query: &bson::Document) -> std::result::Result<Option<Record>, String> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => find_one_and_delete_internal::<_, _, true, true>(self.db, &self.config, query),
            (true, false) => find_one_and_delete_internal::<_, _, true, false>(self.db, &self.config, query),
            (false, false) => find_one_and_delete_internal::<_, _, false, false>(self.db, &self.config, query),
            (false, true) => find_one_and_delete_internal::<_, _, false, true>(self.db, &self.config, query),
        }
    }

    fn get_indexes(&mut self) -> Result<Vec<Index>, String> {
        get_indexes_internal(self.db, &self.config)
    }

    fn insert_one(&mut self, document: &bson::Document) -> std::result::Result<Option<Record>, String> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => insert_one_internal::<_, _, true, true>(self.db, &self.config, document),
            (true, false) => insert_one_internal::<_, _, true, false>(self.db, &self.config, document),
            (false, false) => insert_one_internal::<_, _, false, false>(self.db, &self.config, document),
            (false, true) => insert_one_internal::<_, _, false, true>(self.db, &self.config, document),
        }
    }

    fn insert_many(&mut self, documents: &Vec<bson::Document>) -> std::result::Result<(), String> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => insert_many_internal::<_, _, true, true>(self.db, &self.config, documents),
            (true, false) => insert_many_internal::<_, _, true, false>(self.db, &self.config, documents),
            (false, false) => insert_many_internal::<_, _, false, false>(self.db, &self.config, documents),
            (false, true) => insert_many_internal::<_, _, false, true>(self.db, &self.config, documents),
        }
    }

    fn reindex(&mut self) -> std::result::Result<(), String> {
        reindex_internal(self.db, &self.config)
    }

    fn replace_one(&mut self, query: &bson::Document, replacement: &bson::Document, skip: i64) -> std::result::Result<Option<Record>, String> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => replace_one_internal::<_, _, true, true>(self.db, &self.config, query, replacement, skip),
            (true, false) => replace_one_internal::<_, _, true, false>(self.db, &self.config, query, replacement, skip),
            (false, false) => replace_one_internal::<_, _, false, false>(self.db, &self.config, query, replacement, skip),
            (false, true) => replace_one_internal::<_, _, false, true>(self.db, &self.config, query, replacement, skip),
        }
    }

    fn update_one(&mut self, query: &bson::Document, update: &bson::Document, skip: i64, upsert: bool) -> std::result::Result<Option<Record>, String> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => update_one_internal::<_, _, true, true>(self.db, &self.config, query, update, skip, upsert),
            (true, false) => update_one_internal::<_, _, true, false>(self.db, &self.config, query, update, skip, upsert),
            (false, false) => update_one_internal::<_, _, false, false>(self.db, &self.config, query, update, skip, upsert),
            (false, true) => update_one_internal::<_, _, false, true>(self.db, &self.config, query, update, skip, upsert),
        }
    }

    /// This function update all documents match the `query` by the `update` object. If `upsert` is true, and no documents are found by
    /// query, we will create a new document using the `update` object.
    fn update_many(&mut self, query: &bson::Document, update: &bson::Document, limit: i64, skip: i64, upsert: bool) -> Result<i64, String> {
        match (self.config.should_hash_document, self.config.should_log_last_modified) {
            (true, true) => update_many_internal::<_, _, true, true>(self.db, &self.config, query, update, limit, skip, upsert),
            (true, false) => update_many_internal::<_, _, true, false>(self.db, &self.config, query, update, limit, skip, upsert),
            (false, false) => update_many_internal::<_, _, false, false>(self.db, &self.config, query, update, limit, skip, upsert),
            (false, true) => update_many_internal::<_, _, false, true>(self.db, &self.config, query, update, limit, skip, upsert),
        }
    }
}

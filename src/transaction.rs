
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
use crate::base::*;

pub struct TransactionCollection<'conn, const H: bool, const L: bool, const E: bool, const C: bool> {
    pub name: String,
    pub db: Weak<RefCell<rusqlite::Transaction<'conn>>>,
    pub table_name: String,
}

impl<'conn, const H: bool, const L: bool, const E: bool, const C: bool> TransactionCollection<'conn, H, L, E, C> {
    /*pub fn find(&mut self, query: &serde_json::Value, options: &Option<SearchOption>, f: &mut dyn FnMut(&Record) -> std::result::Result<(), &'static str>) -> std::result::Result<(), &str> {
        let db_internal = self.db.upgrade().unwrap();
        let conn = db_internal.borrow_mut();
        return find_internal::<_, true, true>(&self.table_name, &conn, query, options, f);
    }*/

}
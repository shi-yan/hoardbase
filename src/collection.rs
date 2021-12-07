use bson::ser::Serializer;
use bson::Bson;
use bson::Document;
use rusqlite::params_from_iter;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use rusqlite::params;
use crate::query_translator::QueryTranslator;

pub struct Collection {
    pub connection: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>,
    pub name: String,
    pub table_name: String,
}

impl Collection {
    pub fn find(&mut self) {
        println!("call find for collection {}", self.name);
    }

    pub fn count() {}
    pub fn create_index() {}
    pub fn delete_one() {}
    pub fn delete_many() {}

    pub fn distinct(&mut self) {}
    pub fn drop() {}

    pub fn drop_index() {}
    pub fn ensure_index() {}
    pub fn explain() {}

    pub fn find_one(&mut self, query: serde_json::Value) -> std::result::Result<serde_json::Value, &str> {

        let where_str: String = QueryTranslator{}.query_document(&query).unwrap();
        let mut params: Vec<u64> = Vec::new();

        params.push(10);

        println!("where_str {}", &where_str);
        
        let mut conn = self.connection.borrow_mut();
        let tx_inner = conn.transaction().unwrap();//params_from_iter(params.iter())
        let value = Collection::_find_one_transaction(&self.table_name, &tx_inner, &where_str, params![] ).unwrap();
        tx_inner.commit().unwrap();

        let bson_doc: bson::Bson = bson::from_reader(value.1.as_slice()).unwrap();
        let json_doc: serde_json::Value = bson_doc.clone().into();

        Ok(json_doc)
    }

    pub fn find_one_and_delete() {}
    pub fn find_one_and_replace() {}
    pub fn find_one_and_update() {}
    pub fn find_and_modify() {}
    pub fn get_indexes() {}

    pub fn insert_one(&mut self, document: serde_json::Value) -> std::result::Result<(), &str> {
        let mut conn = self.connection.borrow_mut();
        let bson_doc = bson::ser::to_document(&document).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        bson_doc.to_writer(&mut bytes).unwrap();
        let tx_inner = conn.transaction().unwrap();
        Collection::_insert_one_transaction(&self.table_name, &tx_inner, bytes.as_ref());
        tx_inner.commit().unwrap();
        Ok(())
    }

    pub fn insert_many() {}

    pub fn reindex() {}
    pub fn replace_one() {}
    pub fn remove() {}

    pub fn update_one() {}
    pub fn update_many() {}

    fn _insert_one_transaction(collection_name: &str, tx: &rusqlite::Transaction, blob: &[u8]) {
        let mut stmt = tx.prepare_cached(&format!("INSERT INTO [{}] (raw) VALUES (?1)", collection_name)).unwrap();
        stmt.execute(&[blob]).unwrap();
    }

    fn _find_one_transaction<P>(collection_name: &str, tx: &rusqlite::Transaction, where_str: &str, values: P) -> Result<(u64, Vec<u8>), &'static str>
    where
        P: rusqlite::Params,
    {
        let mut stmt = tx.prepare_cached(&format!("SELECT * FROM [{}] WHERE {} LIMIT 1", collection_name, where_str)).unwrap();
        let row = stmt.query_row(values, |row| Ok((row.get::<_, u64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap())));
        Ok(row.unwrap())
    }
}

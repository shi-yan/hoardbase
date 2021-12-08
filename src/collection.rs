use bson::ser::Serializer;
use bson::Bson;
use bson::Document;
use rusqlite::params_from_iter;
use serde_json::Value;
use std::cell::RefCell;
use std::rc::Rc;
use rusqlite::params;
use serde_json::json;
use slugify::slugify;

use crate::query_translator::QueryTranslator;

pub struct Collection {
    pub connection: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>,
    pub name: String,
    pub table_name: String,
}

fn translate_index_config(config: &serde_json::Value, scope: &str, fields: &mut Vec<(String, i8)>) -> std::result::Result<(), &'static str> {
    if config.is_object() {
        for (key, value) in config.as_object().unwrap().iter() 
        {
           if value.is_object() {
               return translate_index_config(&value, &format!("{}{}.", scope, key), fields);
           }
           else if value.is_number() {
              let order = value.as_i64().unwrap();

              if order != -1 && order != 1 {
                  return Err("Invalid order");
              }

              fields.push((format!("{}{}", scope, key), order as i8));
              return Ok(());
           }
           else {
                return Err("Invalid index config");
           }
        }
        Err("no members in index config")
    }
    else {
        Err ("Index config must be an object")
    }
}

impl Collection {
    pub fn find(&mut self) {
        println!("call find for collection {}", self.name);
    }

    pub fn count_document(&mut self, query: &serde_json::Value) -> std::result::Result<i64, &str> {
        //todo implement skip limit
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator{}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let conn = self.connection.borrow_mut();
        let mut stmt = conn.prepare_cached(&format!("SELECT COUNT(1) FROM [{}] {};", &self.table_name, if where_str.len() > 0 {format!("WHERE {}", &where_str)} else {String::from("")})).unwrap();
        let count = stmt.query_row(params_from_iter(params.iter()), |row| Ok(row.get::<_, i64>(0).unwrap())).unwrap();
        Ok(count)
    }
    
    pub fn create_index(&mut self, config: &serde_json::Value, is_unique: bool) ->std::result::Result<(), String> {
        //todo implement type and size index
        let mut fields:Vec<(String, i8)> = Vec::new();

        let result = translate_index_config(&config, "", &mut fields);

        if let Err(e) = result {
            return Err(String::from(e));
        }
        let conn = self.connection.borrow_mut();
        let mut index_name = String::new();
        let mut config_str  = String::new();
        for field in fields {
            if config_str.len() > 0 {
                config_str.push_str(",");
            }
            config_str.push_str(&format!("json_field('{}', raw) {}", field.0, if field.1 == 1 { "ASC" } else { "DESC" }));
            index_name.push_str(field.0.as_str());
            index_name.push_str("_");
        }

        index_name = slugify!(index_name.as_str(), separator = "_");

        //println!("CREATE {} INDEX IF NOT EXISTS {} ON [{}]({});", if is_unique {"UNIQUE"} else {""},index_name , &self.table_name, &config_str);

        match conn.execute(&format!("CREATE {} INDEX IF NOT EXISTS {} ON [{}]({});", if is_unique {"UNIQUE"} else {""}, index_name, &self.table_name, &config_str), []) {
            Ok(_) => Ok(()),
            Err(e) => Err(e.to_string())
        }
    }

    pub fn delete_one() {}
    pub fn delete_many() {}

    pub fn distinct(&mut self) {}
    pub fn drop() {}

    pub fn drop_index() {}
    pub fn ensure_index() {}
    pub fn explain() {}

    pub fn find_one(&mut self, query: &serde_json::Value) -> std::result::Result<serde_json::Value, &str> {
        let mut params = Vec::<rusqlite::types::Value>::new();
        let where_str: String = QueryTranslator{}.query_document(&query, &mut params).unwrap();
        //println!("where_str {}", &where_str);
        let conn = self.connection.borrow_mut();
        let mut stmt = conn.prepare_cached(&format!("SELECT * FROM [{}] {} LIMIT 1;", &self.table_name, if where_str.len() > 0 {format!("WHERE {}", &where_str)} else {String::from("")})).unwrap();
        let row = stmt.query_row(params_from_iter(params.iter()), |row| Ok((row.get::<_, i64>(0).unwrap(), row.get::<_, Vec<u8>>(1).unwrap()))).unwrap();
        let mut bson_doc: bson::Document = bson::from_reader(row.1.as_slice()).unwrap();
        bson_doc.insert("_id", row.0);
        let json_doc: serde_json::Value = bson::Bson::from(&bson_doc).into();
        
        Ok(json_doc)
    }

    pub fn find_one_and_delete() {}
    pub fn find_one_and_replace() {}
    pub fn find_one_and_update() {}
    pub fn find_and_modify() {}
    pub fn get_indexes() {}

    pub fn insert_one(&mut self, document: &serde_json::Value) -> std::result::Result<(), &str> {
        let bson_doc = bson::ser::to_document(&document).unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        bson_doc.to_writer(&mut bytes).unwrap();
        let conn = self.connection.borrow_mut();
        let mut stmt = conn.prepare_cached(&format!("INSERT INTO [{}] (raw) VALUES (?1)", &self.table_name)).unwrap();
        let bytes_ref:&[u8] = bytes.as_ref();
        stmt.execute(&[bytes_ref]).unwrap();
        Ok(())
    }

    pub fn insert_many() {}

    pub fn reindex() {}
    pub fn replace_one() {}
    pub fn remove() {}

    pub fn update_one() {}
    pub fn update_many() {}
}

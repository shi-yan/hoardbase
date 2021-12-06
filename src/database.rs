use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use crate::collection::Collection;

pub struct Database {
    connection: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>,
    collections: HashMap<String, std::rc::Rc<std::cell::RefCell<Collection>>>,
}

impl Database {
    pub fn open(path: &str) -> std::result::Result<Database, &str> {
        if let Ok(conn) = rusqlite::Connection::open(path) {
            let mut connection = Database {
                connection: std::rc::Rc::new(std::cell::RefCell::new(conn)),
                collections: HashMap::new(),
            };
            connection.init();
            Ok(connection)
        } else {
            Err("Unable to open db.")
        }
    }

    pub fn path(&self) -> Option<String> {
        let path_conn = self.connection.borrow();
        Some(path_conn.path().unwrap().to_string_lossy().into_owned())
    }

    fn init(&mut self) {
        let conn = self.connection.borrow();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _hoardbase (
                      id              INTEGER PRIMARY KEY,
                      collection      TEXT NOT NULL,
                      type            INTEGER NOT NULL,
                      table_name      TEXT UNIQUE NOT NULL
                      )",
            [],
        )
        .unwrap();

        let mut stmt = conn.prepare("SELECT * FROM _hoardbase WHERE type=0").unwrap();
        let mut rows = stmt.query([]).unwrap();
        while let Ok(row_result) = rows.next() {
            if let Some(row) = row_result {
                let collection: String = row.get(1).unwrap();
                let table_name: String = row.get(3).unwrap();

                println!("{} {}", collection, table_name);

                self.collections.insert(
                    collection.to_string(),
                    std::rc::Rc::new(std::cell::RefCell::new(Collection {
                        name: collection.to_string(),
                        connection: self.connection.clone(),
                        table_name: table_name.to_string(),
                    })),
                );
            } else {
                break;
            }
        }
    }

    fn _create_collection_transaction(collection_name: &str, tx: &rusqlite::Transaction) {
        tx.execute(
            &format!(
                "CREATE TABLE [{}] (
                      _id              INTEGER PRIMARY KEY,
                      raw             BLOB NOT NULL
                      )",
                collection_name
            ),
            [],
        )
        .unwrap();

        let mut stmt = tx.prepare_cached("INSERT INTO _hoardbase (collection ,type, table_name) VALUES (?1, ?2, ?3)").unwrap();
        stmt.execute(&[collection_name, "0", collection_name]).unwrap();
    }

    pub fn create_collection(&mut self, collection_name: &str) -> Result<&std::rc::Rc<std::cell::RefCell<Collection>>, &str> {
        if self.collections.contains_key(collection_name) {
            Ok(self.collections.get(collection_name).clone().unwrap())
        } else {
            let mut conn = self.connection.borrow_mut();

            let tx_inner = conn.transaction().unwrap();
            Database::_create_collection_transaction(collection_name, &tx_inner);
            tx_inner.commit().unwrap();

            self.collections.insert(
                collection_name.to_string(),
                std::rc::Rc::new(std::cell::RefCell::new(Collection {
                    name: collection_name.to_string(),
                    connection: self.connection.clone(),
                    table_name: collection_name.to_string(),
                })),
            );
            Ok(self.collections.get(collection_name).clone().unwrap())
        }
    }

    pub fn collection(&mut self, collection_name: &str) -> Result<&std::rc::Rc<std::cell::RefCell<Collection>>, &str> {
        if self.collections.contains_key(collection_name) {
            Ok(self.collections.get(collection_name).clone().unwrap())
        } else {
            Err("No collection found")
        }
    }

    pub fn list_collections(&self) -> Vec<&std::rc::Rc<std::cell::RefCell<Collection>>> {
        let mut collections = Vec::new();
        for collection in self.collections.values() {
            collections.push(collection);
        }
        collections
    }

    pub fn drop_collection(&self) {}
    pub fn rename_collection(&self) {}
}


use std::sync::Arc;
use std::cell::RefCell;

pub struct Connection
{
    connection: rusqlite::Connection
}

impl Connection {
    pub fn open(path: &str) -> std::result::Result<Connection, &str> {
        if let Ok(conn) = rusqlite::Connection::open(path) {
     
     
            Ok(Connection{connection:  conn })
        }
        else
        {
            Err("Unable to open db.")
        }
    }

    pub fn path(&self) -> Option<&std::path::Path> {
        let path = self.connection.path();
        path
       
    }

    pub fn execute(&self) {

        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS _hoardbase (
                      id              INTEGER PRIMARY KEY,
                      name            TEXT NOT NULL,
                      type            INTEGER NOT NULL
                      )",
            [],
        ).unwrap();

        let mut stmt = self.connection.prepare("SELECT name FROM sqlite_master WHERE name=\"_hoardbase\";").unwrap();
        let map =  stmt.query_map([], |row| {
            let name:String = row.get(0).unwrap();
            println!("{:?}", name);
            Ok(name)
        }).unwrap();

        println!("{:?}", map.last());
    }

    pub fn create(&self, collection_name: &str) {
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS test.coll.com (
                      id              INTEGER PRIMARY KEY,
                      name            TEXT NOT NULL,
                      type            INTEGER NOT NULL
                      )",
            [collection_name],
        ).unwrap();
    }
}
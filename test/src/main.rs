#[macro_use]
extern crate ouroboros;

use std::rc::Weak;
use std::rc::Rc;
use std::cell::RefCell;


#[derive(Clone)]
struct Connection {
    conn: Weak<RefCell<rusqlite::Connection>>,
}

struct Database {
    pub db: Rc<RefCell<rusqlite::Connection>>,
    pub conns: Vec< Connection >
}

struct Transaction<'a> {
    pub trans: &'a mut rusqlite::Transaction<'a>
}

trait ConnectionInterface {
    fn find(&mut self) -> Option<String>;
}

impl ConnectionInterface for Connection {
    fn find(&mut self) -> Option<String> {
        let conn_ptr = self.conn.upgrade().unwrap();
        let conn = conn_ptr.borrow_mut();
        let mut stmt = conn.prepare("SELECT name FROM users WHERE id = ?1").unwrap();
        stmt.execute([1]).unwrap();
        Some("".to_string())
    }
}

impl Database {

    fn get_connection(&mut self) -> Connection {
        self.conns.push(Connection { conn: Rc::downgrade(&self.db) });
        self.conns[0].clone()
    }

    /*fn transaction(&mut self) -> Result<(), &str>{
        self.trans = Some(&self.db.borrow_mut().transaction().unwrap());
        Ok(())
    }*/
}

fn main() {
    println!("Hello, world!");

    let mut db = Database {
        db: Rc::new( RefCell::new(rusqlite::Connection::open("foo.db").unwrap())),
        conns: Vec::new()
    };
    {
        let mut conn = db.db.borrow_mut();
        let mut tran1 = conn.transaction().unwrap();
        {
            let _tran = Transaction {trans: &mut tran1};
        }
        //tran.commit().unwrap();
    }
    db.get_connection().find();

}

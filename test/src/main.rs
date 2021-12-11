
struct Connection<'a> {
    conn: &'a rusqlite::Connection,
}

struct Database<'a> {
    db: rusqlite::Connection,
    conns: Vec<Connection<'a>>,
}


impl<'a> Database<'a> {

    fn get_connection(&self) -> &'a mut Connection<'a> {
        self.conns.push(Connection { conn: &self.db });
        &mut self.conns[1]
    }
}

fn main() {
    println!("Hello, world!");

    let db = Database {
        db: rusqlite::Connection::open("foo.db").unwrap(),
    };


}

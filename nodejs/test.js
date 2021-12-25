const Database = require('./index')

console.log(Database)

let db = new Database("test.db")

db.createCollection("test")

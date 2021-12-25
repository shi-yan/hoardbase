const Database = require('./index')

console.log(Database)

let db = new Database("test.db")

let col = db.createCollection("test")

col.insertOne({data: "test", age: 23})

const Database = require('./index')

console.log(Database)

let db = new Database("test.db")

let col = db.createCollection("test")

col.insertOne({data: "test", age: 23, test_arr: [1, 2, 3], test_obj: {a: 1, b: 2}})

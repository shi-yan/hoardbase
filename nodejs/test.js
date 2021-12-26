const Database = require('./index')
const fs = require('fs')


describe('Hoardbase', () => {
    let db = null
    let path = 'test.db'
    beforeAll(() => {
        if (fs.existsSync(path)) {
            fs.unlinkSync(path)
        }
        db = new Database("test.db")
    });
    afterAll(() => {
        if (fs.existsSync(path)) {
            fs.unlinkSync(path)
        }
    });
    beforeEach(() => {

    });
    afterEach(() => {

    });
    test('test insert_one', () => {
        let col = db.createCollection("test")
        let r = col.insertOne({ data: "test", age: 23, test_arr: [1, 2, 3], test_obj: { a: 1, b: 2 } })
        expect(r._id).toBe(1);
    });
});


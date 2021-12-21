import hoardbase

db = hoardbase.Database.open('test.db')

col = db.create_collection('test')

r = col.insert_one({'name': 'test'})

print(r.id, r.hash, r.last_modified)

#r2 = db.collection('test').insert_one({'name': 'test2'})

#print(r2.id, r2.hash, r2.last_modified)
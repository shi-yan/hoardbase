import hoardbase
import unittest
import os


class TestHoardbase(unittest.TestCase):

    def setUp(self):
        self.path = 'test.db'
        if os.path.exists(self.path):
            os.remove(self.path)
        self.db = hoardbase.Database.open('test.db')

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_insert_one(self):
        col = self.db.create_collection('test')
        r = col.insert_one({'name': 'test'})
        print(r.id, r.hash, r.last_modified)
        self.assertEqual(r.id, 1)
        def process(r, b):
            print("called in py:", r)
            print(r.id, r.hash, r.last_modified)
        self.db.collection('test').find({'name': 'test'}, process)

        

if __name__ == '__main__':
    unittest.main()

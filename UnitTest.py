import unittest
from DATABASE import DB, Options
from Txn import Tx
import const

default_Options = Options(
    page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
)

class TestDatabase(unittest.TestCase):
    def setUp(self):
        """Initialize the database before each test."""
        self.db = DB.open("TestDB", default_Options)
        self.tx = self.db.write_tx()
        self.collection_name = "TestCollection"
        self.collection, _ = self.tx.Create_Collection(self.collection_name.encode())
    
    def tearDown(self):
        """Clean up after each test."""
        self.db.close()

    def test_insert_and_retrieve(self):
        """Test inserting and retrieving data."""
        key = b"test_key"
        value = b"test_value"
        self.collection.put(key, value)
        
        item, err = self.collection.find(key)
        self.assertIsNone(err, "Error should be None")
        self.assertIsNotNone(item, "Item should be found")
        self.assertEqual(item.key, key, "Keys should match")
        self.assertEqual(item.value, value, "Values should match")
    
    def test_update_value(self):
        """Test updating an existing value."""
        key = b"update_key"
        initial_value = b"initial_value"
        updated_value = b"updated_value"
        
        self.collection.put(key, initial_value)
        self.collection.put(key, updated_value)
        
        item, err = self.collection.find(key)
        self.assertIsNone(err, "Error should be None")
        self.assertIsNotNone(item, "Item should be found")
        self.assertEqual(item.key, key, "Keys should match")
        self.assertEqual(item.value, updated_value, "Updated value should match")
    
    def test_delete(self):
        """Test deleting a key."""
        key = b"delete_key"
        value = b"delete_value"
        
        self.collection.put(key, value)
        self.collection.remove(key)
        
        item, err = self.collection.find(key)
        self.assertIsNone(err, "Error should be None")
        self.assertIsNone(item, "Item should be deleted and not found")
    
    def test_non_existent_key(self):
        """Test behavior for non-existent key."""
        key = b"non_existent_key"
        item, err = self.collection.find(key)
        self.assertIsNone(err, "Error should be None")
        self.assertIsNone(item, "Item should not be found")
    
    def test_error_handling(self):
        """Test error handling for invalid operations."""
        # Simulate read-only transaction
        self.tx = self.db.read_tx(writable=False)
        key = b"invalid_key"
        value = b"invalid_value"
        err = self.collection.put(key, value)
        self.assertEqual(err, const.writeInsideReadTxErr, "Error should be writeInsideReadTxErr")

if __name__ == "__main__":
    unittest.main()

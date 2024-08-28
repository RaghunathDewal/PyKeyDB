from typing import Optional
from DATABASE import DB, Options  

default_Options = Options(
    page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
)

def main():
    db=None
    try:
        db = DB.open("Demo7", default_Options)
        
    
        tx = db.write_tx()
        
        collection_name = "Demo7Collection"
        created_collection, _ = tx.Create_Collection(collection_name.encode())
        
       
        key_value_pairs = [
            (b"key0", b"value0"),
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
            (b"key4", b"value4")
        ]
        
        # Insert each key-value pair into the collection
        for key, value in key_value_pairs:
            created_collection.put(key, value)
        
        # Retrieve and print each item
        for key, _ in key_value_pairs:
            item, _ = created_collection.find(key)
            if item is None:
                print(f"Item with key {key.decode()} not found.")
            else:
                print(f"key is: {item.key.decode()}, value is: {item.value.decode()}")
        
        # Commit the transaction
        tx.Commit()
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()


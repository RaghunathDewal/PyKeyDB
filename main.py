from typing import Optional
from DataBase.DATABASE import DB, Options  

default_Options = Options(
    page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
)

def main():
    db = None
    try:
        # Open the database
        db = DB.open("Demo7", default_Options)
        
        # Start a write transaction
        tx = db.write_tx()
        
        collection_name = "Demo7Collection"
        created_collection, _ = tx.Create_Collection(collection_name.encode())
        
        new_key = b"key0"
        new_val = b"value0"
        created_collection.put(new_key, new_val)
        
        
        item, _ = created_collection.find(new_key)
        if item is None:
            print(f"Item with key {new_key.decode()} not found.")
        else:
            print(f"key is: {item.key.decode()}, value is: {item.value.decode()}")
        
        tx.Commit()
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()


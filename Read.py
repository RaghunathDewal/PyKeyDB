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
        db = DB.open("Demo8", default_Options)
        
    
        read = db.read_tx()
        collection_name = b"First_Collection"
        read_collection, err = read.Get_Collection(collection_name)
        if err or read_collection is None:
            print(f"Error reading back collection: {err}")
        else:
            print(f"Read back collection: name={read_collection.name}, root={read_collection.root}")
            item, err = read_collection.find(b"key0")
            if item:
                print(f"Read back item: key={item.key}, value={item.value}")
            else:
                print(f"Failed to read back item: {err}")

        print("Committing write transaction...")
        read.Commit()
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    main()


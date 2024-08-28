import os
from dal import DAL, Options
from Txn import Tx
from const import PAGE_SIZE

default_Options = Options(
    page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
)

def main():
    # Define the path for the database
    db_path = "example.db"

    # Check if the database file already exists
    if os.path.exists(db_path):
        os.remove(db_path)

    # Open the database (unpack the returned tuple)
    db, err = DAL.new_dal(db_path, default_Options)

    if err is not None:
        print(f"Error opening database: {err}")
        return

    # Define the collection name as bytes
    collection_name = b'DemoCollection'

    # Create a new collection directly (without using a Txn)
    print(f"Creating collection: {collection_name}")
    
    # Start a write transaction
    TX = Tx(db, True)
    collection, err = TX.Create_Collection(collection_name)

    if err is not None:
        print(f"Error creating collection: {err}")
        return
    
    print(f"Collection created: name={collection_name}")

    # Optionally, you could add key-value pairs to the collection
    key, value = b'key1', b'value1'
    print(f"Inserting key-value: {key} -> {value}")
    collection.put(key, value)
    
    # Read the collection back to verify creation
    fetched_value = collection.get(key)
    print(f"Retrieved value for {key}: {fetched_value}")

    # Commit the transaction
    TX.Commit()

    # Close the database
    db.close()

if __name__ == "__main__":
    main()
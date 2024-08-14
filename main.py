from DATABAS import DB, Options
from Txn  import Tx
def main():
    # Open the database
    db = DB.open("Demo7", Options(min_fill_percent=0.5, max_fill_percent=1.0))

    # Start a write transaction
    tx = db.write_tx()

    # Create a collection
    collection_name = b"Demo7Collection"
    created_collection, _ = Tx.Create_Collection(collection_name)

    # Put a key-value pair into the collection
    new_key = b"key0"
    new_val = b"value0"
    created_collection.put(new_key, new_val)

    # Commit the transaction and close the database
    Tx.Commit()
    DB.close()

    # Reopen the database
    db = DB.open("Demo7", Options(min_fill_percent=0.5, max_fill_percent=1.0))

    # Start a read transaction
    tx = db.read_tx()

    # Get the collection and find the item by key
    created_collection, _ = Tx.Get_Collection(collection_name)
    item, _ = created_collection.find(new_key)

    # Commit the transaction and close the database
    Tx.Commit()
    DB.close()

    # Print the key and value
    print(f"key is: {item.key.decode('utf-8')}, value is: {item.value.decode('utf-8')}")

if __name__ == "__main__":
    main()
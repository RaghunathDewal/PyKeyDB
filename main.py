from DATABASE import DB, Options

default_Options = Options(
    page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
)

def main():
    db = None
    try:
        print("Opening database...")
        db = DB.open("Trial", default_Options)
        print(f"Root page number after database open: {db.root}")

        print("Starting write transaction...")
        write_tx = db.write_tx()

        collection_name = b"First_Collection"

        print(f"Creating collection: {collection_name}")
        created_collection, err = write_tx.Create_Collection(collection_name)
       

        print(f"Created collection: name={created_collection.name}, root={created_collection.root}")


        key_value_pairs = [
            (b"key0", b"value0"),
            (b"key1", b"value1"),
            (b"key2", b"value2"),
            (b"key3", b"value3"),
            (b"key4", b"value4")
        ]

        print("Inserting key-value pairs...")
        for key, value in key_value_pairs:
            err = created_collection.put(key, value)
            if err:
                print(f"Error putting key {key.decode()}: {err}")
            else:
                print(f"Successfully put key: {key.decode()}")

        
        err = write_tx.Commit()
        if err:
            print(f"Error committing write transaction: {err}")
            return
        print("Write transaction committed successfully")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if db:
            print("Closing database...")
            db.close()
            return db

if __name__ ==  "__main__":
    main()

import os
from dal import DAL,Options
from collection import Collection
from node import Node, Item

def main():
    # Set options similar to the Go code
    options = Options(
        page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
    )
    
    # Initialize DAL (Data Access Layer)
    dal, err = DAL.new_dal("./mainTest", options)
    if err:
        print(f"Error initializing DAL: {err}")
        return

    # Create a new Collection and set its DAL
    c = Collection.new_collection(b"collection1", dal.root)
    c.dal = dal
    
    # Add items to the collection
    err = c.put(b"Key1", b"Value1")
    if err: print(f"Error putting Key1: {err}")
    err = c.put(b"Key2", b"Value2")
    if err: print(f"Error putting Key2: {err}")
    err = c.put(b"Key3", b"Value3")
    if err: print(f"Error putting Key3: {err}")
    err = c.put(b"Key4", b"Value4")
    if err: print(f"Error putting Key4: {err}")
    err = c.put(b"Key5", b"Value5")
    if err: print(f"Error putting Key5: {err}")
    err = c.put(b"Key6", b"Value6")
    if err: print(f"Error putting Key6: {err}")

    # Find an item in the collection
    item, err = c.find(b"Key1")
    if err:
        print(f"Error finding Key1: {err}")
    else:
        if item:
            print(f"Key is: {item.key.decode()}, Value is: {item.value.decode()}")
        else:
            print("Key not found")

    # Close the DAL
    close_err = dal.close()
    if close_err:
        print(f"Error closing DAL: {close_err}")

if __name__ == "__main__":
    main()
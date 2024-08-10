from dal import DAL
from node import Node

def main():
    # Initialize db
    dal, err = DAL.new_dal("./mainTest", page_size=4096)
    if err:
        print(f"Error initializing DAL: {err}")
        return

    if dal.root is None:
        print("Database is empty. Please insert some data first.")
        dal.close()
        return

    node, err = dal.get_node(dal.root)
    if err:
        print(f"Error getting root node: {err}")
        dal.close()
        return

    index, containing_node, err = node.find_key(b"Key1")
    if err:
        print(f"Error finding key: {err}")
        dal.close()
        return

    if containing_node is None:
        print("Key not found")
    else:
        res = containing_node.items[index]
        print(f"key is: {res.key.decode()}, value is: {res.value.decode()}")
    
    # Close the db
    dal.close()

if __name__ == "__main__":
    main()
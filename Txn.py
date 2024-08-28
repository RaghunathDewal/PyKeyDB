from typing import TYPE_CHECKING, Tuple, Optional
import const

if TYPE_CHECKING:
    from collection import Collection
    from node import Node, Item
    from DATABASE import DB

class Tx:
    def __init__(self, db: 'DB', write: bool):
        self.dirty_node: dict[int, 'Node'] = {}
        self.pages_to_delete: list[int] = []
        self.allocated_pages: list[int] = []
        self.write = write
        self.db = db

    @classmethod
    def new_tx(cls, db: 'DB', write: bool) -> 'Tx':
        return cls(db, write)

    def new_node(self, items: list['Item'], child_nodes: list[int]) -> 'Node':
        from node import Node
        node = Node.empty_node()
        node.items = items
        node.child_nodes = child_nodes
        node.page_num = self.db.dal.get_nxt_page()
        node.tx = self
        self.allocated_pages.append(node.page_num)
        return node

    def get_node(self, page_num: int) -> 'Node':
        if page_num in self.dirty_node:
            node = self.dirty_node[page_num]
            node.tx = self
            return node

        node,err = self.db.dal.get_node(page_num)
        if err:
            print(f"{err}")
        node.tx = self
        if self.write:
            self.dirty_node[page_num] = node
        return node

    def write_node(self, node: 'Node') -> 'Node':
        self.dirty_node[node.page_num] = node
        node.tx = self
        return node

    def delete_node(self, node: 'Node'):
        self.pages_to_delete.append(node.page_num)

    def getRootCollection(self) -> 'Collection':
        from collection import Collection
        root_Collection = Collection.new_empty_collection()
        root_Collection.root = self.db.dal.meta.root
        root_Collection.tx = self
        return root_Collection

    def Get_Collection(self, name: bytes) -> Tuple[Optional['Collection'], Optional[Exception]]:
        from collection import Collection
        root_collection = self.getRootCollection()
        print(f"Getting collection: {name}")
        
        item, err = root_collection.find(name)
        if err:
            print(f"Error finding collection: {err}")
            return None, err

        if item is None:
            print(f"Collection not found: {name}")
            return None, None

        collection = Collection.new_empty_collection()
        collection.Deserialize(item)
        collection.tx = self
        return collection, None

    def Create_Collection(self, name: bytes) -> Tuple[Optional['Collection'], Optional[Exception]]:
        from collection import Collection  
        if not self.write:
            return None, const.writeInsideReadTxErr

        new_collection_node = self.new_node([], [])
        new_collection = Collection.new_collection(name, new_collection_node.page_num)
        new_collection.tx = self
        
        collection, err = self._create_collection(new_collection)
        if err:
            return None, err
        return collection, None

    def Delete_collection(self, name: bytes):
        if not self.write:
            return const.writeInsideReadTxErr

        root_collection = self.getRootCollection()
        return root_collection.remove(name)

    def _create_collection(self, collection: 'Collection') -> Tuple[Optional['Collection'], Optional[Exception]]:
        collection.tx = self
        collection_item = collection.Serialize()

        root_collection = self.getRootCollection()
        print(f"Adding collection {collection.name} to root")
        
        err = root_collection.put(collection.name, collection_item.value)
        if err:
            print(f"Error putting collection in root: {err}")
            return None, err

        print(f"Collection added to root: {collection.name}")
        return collection, None

    def Rollback(self):
        if not self.write:
            self.db.rwlock.release()
            return

        self.dirty_node = {}
        self.pages_to_delete = []

        for page_num in self.allocated_pages:
            self.db.dal.free_list.release_page(page_num)

        self.allocated_pages = []
        self.db.rwlock.release()

    def Commit(self):
        if not self.write:
            self.db.rwlock.release()
            return None

        try:
            for node in self.dirty_node.values():
                self.db.dal.write_node(node)

            for page_num in self.pages_to_delete:
                self.db.dal.delete_node(page_num)

            self.db.dal.Write_Freelist()

            self.dirty_node = {}
            self.pages_to_delete = []
            self.allocated_pages = []

            self.db.rwlock.release()
            return None
        except Exception as e:
            return str(e)
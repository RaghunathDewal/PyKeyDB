from DB import DB
from node import Node,Item
from collection import Collection
from typing import Tuple,Optional
class Tx():
    def __init__(self,db:'DB',write:bool):
        self.dirty_node:dict[int,Node] = {}
        self.pages_to_delete:list[int]= []
        self.allocated_pages:list[int]= []
        self.write =write
        self.db= db
        self.Collection = Collection
    
    @classmethod
    def new_tx(cls,db,write):
        return cls(db,write)
    
    def new_node(self, items:list[Item],child_nodes: list[int])-> Node:
        node = Node.empty_node()
        node.items= items
        node.child_nodes = child_nodes
        node.page_num = self.db.dal.get_nxt_page()
        node.tx= self
        self.allocated_pages.append(node.page_num)
        return node
    
    def get_node(self,page_num:int)-> Node:
        if page_num in self.dirty_node:
            return self.dirty_node[page_num]
        
        node = self.db.dal.get_node(page_num)
        node.tx =self
        return node
    
    def write_node(self, node:Node)->Node:
        self.dirty_node[node.page_num]= node
        node.tx=self
        return node
    
    def delete_node(self,node:Node):
        self.pages_to_delete.append(node.page_num)
    
    def getRootCollection(self):
        root_Collection = self.Collection.new_empty_collection()
        root_Collection.root= self.db.root
        root_Collection.tx = Tx
        return root_Collection
    
    def find_Collection(self,name:bytes)-> Tuple[Optional['Collection'], Optional[Exception]]:
        root_collection = self.getRootCollection()
        Item,err = root_collection.find(name)
        if err:
            return err,None
        
        if Item == None:
            return None,None
        
        collection = self.Collection.new_empty_collection()
        collection.d



    def Rollback(self):
        if not self.write:
            self.db.rwlock.release()
            return
        
        self.dirty_node= None
        self.pages_to_delete = None

        for page_num in self.allocated_pages:
            self.db.freelist.release_page(page_num)

        
        self.allocated_pages = None
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

            self.db.dal.Write_Freelist(self.db.freelist)

            self.dirty_node = {}
            self.pages_to_delete= []
            self.allocated_pages = []

            self.db.rwlock.release()
            return None
        except Exception as e:
            return str(e)
        




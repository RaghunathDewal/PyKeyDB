import struct
from typing import Tuple ,Optional,List
import const
class Item:
    def __init__(self,key = bytes,value= bytes):
        self.key = key
        self.value= value


class Node:
    def __init__(self,dal = None, page_num=None):
        self.dal=dal
        self.page_num= 0
        self.items=[]
        self.child_nodes= []

    
    @staticmethod
    def new_node_for_serialization(items: List[Item], child_nodes: List[int]) -> 'Node':
        node = Node()
        node.items = items
        node.child_nodes = child_nodes
        return node


    def split_node(self,node_to_split,node_to_split_index:int):
        split_index =self.dal.get_split_index(node_to_split)
        midddle_item = node_to_split.items[split_index]

        if node_to_split.is_leaf():
            new_node , _ =self.write_node(self.dal.new_node(node_to_split.items[split_index+1:],[]))
            node_to_split.items = node_to_split.items[:split_index]
        else:
            new_node , _ = self.write_node(self.dal.new_node(node_to_split.items[split_index+1:], node_to_split.child_nodes[split_index+1:]))
            node_to_split.items =node_to_split.items[:split_index]
            node_to_split.child_nodes = node_to_split.child_nodes[:split_index+1]

        self.add_item(midddle_item,node_to_split_index)

        if len(self.child_nodes) == node_to_split_index+1:
            self.child_nodes.append(new_node.page_num)
        else:
            self.child_nodes =self.child_nodes[:node_to_split_index+1]+ [new_node.page_num] + self.child_nodes[node_to_split_index+1:]

        self.write_nodes(self,node_to_split)




    def write_node(self,node):
        return self.dal.write_node(node)
    
    def write_nodes(self, *nodes):
        for node in nodes:
            self.write_node(node)

    def get_node(self,page_num):
        return self.dal.get_node(page_num)
    

    def find_key_in_node(self,key:bytes) -> Tuple[bool, int]:
        for i, existing_items in enumerate(self.items):
            existing_key= existing_items.key
            res=(existing_key>key)-(existing_key<key)

            if res == 0:
                return True,i
            
            if res ==1:
                return False,i
            
        return False,len(self.items)
    
    def find_key(self, key: bytes, exact: bool) -> Tuple[int, Optional["Node"], List[int], Optional[Exception]]:
        ancestors_indexes = [0]  
        index, node, err = self.find_key_helper(key, exact, ancestors_indexes)
        if err:
            return -1, None, [], err
        return index, node, ancestors_indexes, None
       
    
    def find_key_helper(self, key: bytes, exact: bool, ancestors_indexes: List[int]) -> Tuple[int, Optional["Node"], Optional[Exception]]:
        was_found, index = self.find_key_in_node(key)
        if was_found:
            return index, self, None
        
        if self.is_leaf():
            return -1, None, None
        
        # Update ancestors_indexes before traversing to the next child node
        ancestors_indexes.append(index)
        
        try:
            next_child = self.get_node(self.child_nodes[index])
        except Exception as e:
            return -1, None, e
        
        return next_child.find_key_helper(key, exact, ancestors_indexes)
    
    

    @staticmethod
    def empty_node():
        return Node()
    
    @staticmethod
    def new_item(key : bytes, value: bytes):
        return Item(key,value)
    
    def is_leaf(self):
        return len(self.child_nodes)==0
    
    def serialize(self, buf: bytearray) -> bytearray:
        pos = 0
        # Write number of items
        struct.pack_into('<H', buf, pos, len(self.items))
        pos += 2

        for item in self.items:
            klen = len(item.key)
            vlen = len(item.value)
            # Write key length
            struct.pack_into('<H', buf, pos, klen)
            pos += 2
            # Write key
            buf[pos:pos + klen] = item.key
            pos += klen
            # Write value length
            struct.pack_into('<H', buf, pos, vlen)
            pos += 2
            # Write value
            buf[pos:pos + vlen] = item.value
            pos += vlen

        return buf
    
    def deserialize(self, buf: bytearray):
        pos = 0
        # Read number of items
        item_count, = struct.unpack_from('<H', buf, pos)
        pos += 2

        self.items = []
        
        for _ in range(item_count):
            # Read key length
            klen, = struct.unpack_from('<H', buf, pos)
            pos += 2
            if pos + klen > len(buf):
                raise IndexError("Buffer overflow while reading key")
            key = buf[pos:pos + klen]
            pos += klen

            # Read value length
            vlen, = struct.unpack_from('<H', buf, pos)
            pos += 2
            if pos + vlen > len(buf):
                raise IndexError("Buffer overflow while reading value")
            value = buf[pos:pos + vlen]
            pos += vlen
            
            # Append item
            self.items.append(Item(key, value))

    def element_size(self, i: int)->int:
        size = 0
        size += len(self.items[i].key)
        size += len(self.items[i].value)
        size += const.PAGE_NUM_SIZE
        return size
    def node_size(self):
        # Calculate the node size based on the number of items and their sizes
        size = 0
        size += struct.calcsize('<H')  # For the number of items
        for item in self.items:
            size += struct.calcsize('<H') * 2  # For key_len and value_len
            size += len(item.key) + len(item.value)  # For the actual key and value
        return size
    
    def add_item(self,item,insertion_index:int)-> int:
        if len(self.items) == insertion_index:
            self.items.append(item)
            return insertion_index
        
        self.items = self.items[:insertion_index]+ [item] + self.items[insertion_index:]
        return insertion_index
    
    def is_over_populated(self):
        return self.dal.is_over_populated(self)
    
    def is_under_populated(self):
        return self.dal.is_under_populated(self)
    
    

    
    










   
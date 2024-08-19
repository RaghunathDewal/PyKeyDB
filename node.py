from typing import TYPE_CHECKING, Tuple, Optional, List
import const
import struct

if TYPE_CHECKING:
    from Txn import Tx
    from dal import DAL

class Item:
    def __init__(self, key: bytes = b'', value: bytes = b''):
        self.key = key
        self.value = value

class Node:
    def __init__(self, dal: Optional['DAL'] = None, tx: Optional['Tx'] = None, page_num: Optional[int] = None):
        self.dal = dal
        self.page_num = page_num or 0
        self.items: List[Item] = []
        self.child_nodes: List[int] = []
        self.tx = tx


    
    @staticmethod
    def new_node_for_serialization(items: List[Item], child_nodes: List[int]) -> 'Node':
        node = Node()
        node.items = items
        node.child_nodes = child_nodes
        return node


    def split_node(self, node_to_split, node_to_split_index: int):
        split_index = self.dal.get_split_index(node_to_split)
        middle_item = node_to_split.items[split_index]

        if node_to_split.is_leaf():
            new_node, _ = self.dal.write_node(self.dal.new_node(node_to_split.items[split_index+1:], []))
            node_to_split.items = node_to_split.items[:split_index]
        else:
            new_node, _ = self.dal.write_node(self.dal.new_node(node_to_split.items[split_index+1:], node_to_split.child_nodes[split_index+1:]))
            node_to_split.items = node_to_split.items[:split_index]
            node_to_split.child_nodes = node_to_split.child_nodes[:split_index+1]

        self.add_item(middle_item, node_to_split_index)

        if len(self.child_nodes) == node_to_split_index + 1:
            self.child_nodes.append(new_node.page_num)
        else:
            self.child_nodes = self.child_nodes[:node_to_split_index+1] + [new_node.page_num] + self.child_nodes[node_to_split_index+1:]

        self.dal.write_node(self)
        self.dal.write_node(node_to_split)

    def remove_item_from_leaf(self, index: int) -> Tuple[List[int], Optional[Exception]]:
    # Remove the item at the specified index
        del self.items[index]
        # Write the updated node to storage
        self.write_node(self)
        return [], None

    def remove_item_form_internal(self,index : int)-> Tuple[List[int],Optional[Exception]]:
        affectedNodes = [index]

        aNode,err = self.get_node(self.child_nodes[index])
        if err:
            return [],err
        
        while not aNode.is_leaf():
            transversing_index= len(aNode.child_nodes) -1 
            aNode,err = aNode.get_node(aNode.child_nodes[transversing_index])
            if err:
                return [],err
            affectedNodes.append(transversing_index)

            self.items[index]= aNode.items[-1]
            aNode.items.pop()

            self.write_nodes(self,aNode)

            return affectedNodes,None
        

    def rotateRight(self,a_node: 'Node',p_node:'Node',b_node:'Node',b_node_index : int):
        a_node_item = a_node.items.pop()

        p_node_item_index = b_node_index-1 if b_node_index>0 else 0
        p_node_item = p_node.items[p_node_item_index]
        p_node.items[p_node_item_index]= a_node_item

        b_node.items.insert(0,p_node_item)


        if not a_node.is_leaf():
            child_node_to_shift = a_node.child_nodes.pop()
            b_node.child_nodes.insert(0,child_node_to_shift)

        self.dal.write_node(a_node)
        self.dal.write_node(b_node)
        self.dal.write_node(p_node)


    def left_rotate(self,a_node: 'Node',p_node:'Node',b_node:'Node',b_node_index: int):
        b_node_item = b_node.items.pop(0)

        # Get item from parent node and replace it with b_node_item
        p_node_item_index = b_node_index if b_node_index < len(p_node.items) else len(p_node.items) - 1
        p_node_item = p_node.items[p_node_item_index]
        p_node.items[p_node_item_index] = b_node_item

        a_node.items.append(p_node_item)

        
        if not b_node.is_leaf():
            child_node_to_shift = b_node.child_nodes.pop(0)
            a_node.child_nodes.append(child_node_to_shift)

   
        self.dal.write_node(a_node)
        self.dal.write_node(p_node)
        self.dal.write_node(b_node)

    def merge_node(self,b_node:'Node',b_node_index:int):
        aNode, err = self.dal.get_node(self.child_nodes(b_node_index-1))
        if err:
            print(f"Error Retrieing node:{err}")
            return err
        
        p_node_item = self.items(b_node_index-1)
        self.child_nodes=self.child_nodes[:b_node_index-1] + self.child_nodes[b_node_index+1:]
        if not aNode.is_leaf():
            aNode.child_nodes.extend(b_node.child_nodes)

        self.dal.write_nodes(aNode, self)
        self.dal.delete_node(b_node.page_num)

        return None
    
    def can_spare_an_element(self):
        return len(self.items) > self.dal.min_fill_percent * (self.dal.page_size // self.dal.item_size)
    
    def rebalance_remove(self,unbalanced_node:'Node',unbalanced_node_index:int):
        p_node =self

        if unbalanced_node_index>0:
            left_node,err = self.get_node(p_node.child_nodes[unbalanced_node_index-1])
            if err:
                return err
            if left_node.can_spare_an_element():
                self.rotateRight(left_node,p_node,unbalanced_node,unbalanced_node_index)
                self.dal.write_node(left_node)
                self.dal.write_node(p_node)
                self.dal.write_node(unbalanced_node)
                return None
            

        if unbalanced_node_index<len(p_node.child_nodes) -1 :
            right_node,err = self.get_node(p_node.child_nodes[unbalanced_node_index+1])
            if err:
                return err 
            if right_node.can_spare_an_element():
                self.left_rotate(unbalanced_node,p_node,right_node,unbalanced_node_index)
                self.dal.write_node(right_node)
                self.dal.write_node(p_node)
                self.dal.write_node(unbalanced_node)
                return None
            
        
        if unbalanced_node_index == 0:
            right_node,err = self.get_node(p_node.child_nodes[unbalanced_node_index+1])
            if err:
                return err
            return self.merge_node(right_node,unbalanced_node_index+1)
        
        return self.merge_node(unbalanced_node,unbalanced_node_index) 


        
        

    



    def write_node(self,node):
        return self.dal.write_node(node)
    
    def write_nodes(self, *nodes):
        for node in nodes:
            self.write_node(node)

    def get_node(self,page_num):
        return self.tx.get_node(page_num)
    

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
            return (index, self, None) if not exact else (-1, None, None)
        
        ancestors_indexes.append(index)
        
        try:
            next_child, err = self.dal.get_node(self.child_nodes[index])
            if err:
                return -1, None, err
        except Exception as e:
            return -1, None, e
        
        return next_child.find_key_helper(key, exact, ancestors_indexes)
    
    

    @staticmethod
    def empty_node()-> 'Node':
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

        # Write number of child nodes
        struct.pack_into('<H', buf, pos, len(self.child_nodes))
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

        # Write child nodes
        for child in self.child_nodes:
            struct.pack_into('<Q', buf, pos, child)
            pos += 8

        return buf
    
    def deserialize(self, buf: bytearray):
        pos = 0
        # Read number of items
        item_count, = struct.unpack_from('<H', buf, pos)
        pos += 2

        # Read number of child nodes
        child_count, = struct.unpack_from('<H', buf, pos)
        pos += 2

        self.items = []
        
        for _ in range(item_count):
            # Read key length
            klen, = struct.unpack_from('<H', buf, pos)
            pos += 2
            key = buf[pos:pos + klen]
            pos += klen

            # Read value length
            vlen, = struct.unpack_from('<H', buf, pos)
            pos += 2
            value = buf[pos:pos + vlen]
            pos += vlen
            
            # Append item
            self.items.append(Item(key, value))

        self.child_nodes = []
        for _ in range(child_count):
            child, = struct.unpack_from('<Q', buf, pos)
            self.child_nodes.append(child)
            pos += 8

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
    
    

    
    










   
import struct
from typing import List, Optional, Tuple, Union
from dal import Pgnum, Node, Item
import const
from Txn import Tx
class Collection:
    def __init__(self, name: bytes = b'', root: Pgnum = 0, tx: Optional['Tx'] = None, counter: int = 0):
        self.name: bytes = name
        self.root: Pgnum = root
        self.tx: Optional['Tx'] = tx
        self.counter: int = counter

    @classmethod
    def new_collection(cls, name: bytes, root: Pgnum) -> 'Collection':
        return cls(name, root)

    @classmethod
    def new_empty_collection(cls) -> 'Collection':
        return cls()

    def id(self) -> int:
        if not self.tx.write:
            raise Exception(const.writeInsideReadTxErr)
        
        id_ = self.counter
        self.counter += 1
        return id_

    def Serialize(self)-> 'Item':
        buf= bytearray(const.COLLECTION_SIZE)
        left_pos = 0
        struct.pack_into('<Q',buf,left_pos,self.root)
        left_pos+=const.PAGE_NUM_SIZE
        struct.pack_into('<Q',buf,left_pos,self.counter)
        left_pos += const.COUNTER_SIZE
        return Item(self.name,bytes(buf))
    
    def Deserialize(self,item : 'Item')-> None:
        self.name = item.key

        if len(item.value)!= 0:
            left_pos = 0
            self.root, = struct.unpack_from('<Q',item.value,left_pos)
            left_pos+= const.PAGE_NUM_SIZE

            self.counter, = struct.unpack_from('<Q',item.value,left_pos)
            left_pos += const.COUNTER_SIZE

    def put(self, key: bytes, value: bytes) -> Optional[Exception]:
        if not self.tx.write:
            return const.writeInsideReadTxErr
        
        i = Item(key, value)

        if self.root == 0:
            root = self.tx.write_node(self.tx.new_node([i], []))
            self.root = root.page_num
            
            return None

        root = self.tx.get_node(self.root)
      
        
        insertion_index, node_to_insert_in, ancestors_indexes, err = root.find_key(i.key, False)
        
        if err:
            return err
        
        if node_to_insert_in.items and insertion_index < len(node_to_insert_in.items) and node_to_insert_in.items[insertion_index].key == key:
            node_to_insert_in.items[insertion_index] = i
        else:
            node_to_insert_in.add_item(i, insertion_index)
        node_to_insert_in.write_node(node_to_insert_in)

        ancestors = self.get_nodes(ancestors_indexes)
       

        for i in range(len(ancestors) - 2, -1, -1):
            pnode = ancestors[i]
            node = ancestors[i + 1]
            node_index = ancestors_indexes[i + 1]
            if node.is_over_populated():
                pnode.split_node(node, node_index)

        root_node = ancestors[0]
        if root_node.is_over_populated():
            new_root = self.tx.new_node([], [root_node.page_num])
            new_root.split_node(root_node, 0)
            new_root = self.tx.write_node(new_root)
            self.root = new_root

        return None

    def find(self, key: bytes) -> Tuple[Optional[Item], Optional[Exception]]:
        try:
            root_node = self.tx.get_node(self.root)
            
            index, containing_node, _, err = root_node.find_key(key, True)
            if err:
                return None, err
            if index == -1:
                print(f"Key {key} not found in collection")
                return None, None
            found_item = containing_node.items[index]
            return found_item, None
        except Exception as e:
            print(f"Exception in find: {e}")
            return None, e

    def remove(self, key: bytes) -> Optional[Exception]:
        if not self.tx.write:
            return const.writeInsideReadTxErr
        
        root_node = self.tx.get_node(self.root)
        remove_item_index, node_to_remove_from, ancestors_indexes, err = root_node.find_key(key, True)
        if err:
            return err

        if remove_item_index == -1:
            return None

        if node_to_remove_from.is_leaf():
            node_to_remove_from.remove_item_from_leaf(remove_item_index)
        else:
            affected_nodes = node_to_remove_from.remove_item_form_internal(remove_item_index)
            ancestors_indexes.extend(affected_nodes)

        ancestors = self.get_nodes(ancestors_indexes)

        for i in range(len(ancestors) - 2, -1, -1):
            pnode = ancestors[i]
            node = ancestors[i + 1]
            if node.is_under_populated():
                err = pnode.rebalance_remove(node, ancestors_indexes[i + 1])
                if err:
                    return err

        root_node = ancestors[0]
        if len(root_node.items) == 0 and len(root_node.child_nodes) > 0:
            self.root = ancestors[1].page_num

        return None

    def get_nodes(self, indexes: List[int]) -> List[Node]:
        root = self.tx.get_node(self.root)
        nodes = [root]
        child = root
        for i in range(1, len(indexes)):
            child = self.tx.get_node(child.child_nodes[indexes[i]])
            nodes.append(child)
        return nodes
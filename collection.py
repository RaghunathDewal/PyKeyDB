from typing import Optional, Tuple, List
from dal import DAL, Pgnum
from node import Node, Item

class Collection:
    def __init__(self, name: bytes, root: Pgnum, dal: Optional[DAL] = None):
        self.name: bytes = name
        self.root: Pgnum = root
        self.dal: Optional[DAL] = dal

    @classmethod
    def new_collection(cls, name: bytes, root: Pgnum) -> 'Collection':
        return cls(name, root)

    def find(self, key: bytes) -> Tuple[Optional[Item], Optional[Exception]]:
        try:
            root_node, err = self.dal.get_node(self.root)
            if err:
                return None, err
            index, containing_node, _, err = root_node.find_key(key, True)
            if err:
                return None, err
            if index == -1:
                return None, None
            return containing_node.items[index], None
        except Exception as e:
            return None, e

    def put(self, key: bytes, value: bytes) -> Optional[Exception]:
        item = Item(key, value)

        if self.root == 0:
            root = self.dal.new_node([item], [])
            root, err = self.dal.write_node(root)
            if err:
                return err
            self.root = root.page_num
            self.dal.root = self.root
            return None

        root, err = self.dal.get_node(self.root)
        if err:
            return err

        insertion_index, node_to_insert_in, ancestors_indexes, err = root.find_key(item.key, False)
        if err:
            return err

        if node_to_insert_in is None:
            return ValueError("Node to insert into was not found.")

        # If key already exists
        if node_to_insert_in.items and insertion_index < len(node_to_insert_in.items) and node_to_insert_in.items[insertion_index].key == key:
            node_to_insert_in.items[insertion_index] = item
        else:
            node_to_insert_in.add_item(item, insertion_index)

        _, err = self.dal.write_node(node_to_insert_in)
        if err:
            return err

        # Handle node splitting if necessary
        current_node = node_to_insert_in
        for i in range(len(ancestors_indexes) - 1, -1, -1):
            if current_node.is_over_populated():
                parent_index = ancestors_indexes[i - 1] if i > 0 else -1
                parent_node, err = self.dal.get_node(self.root if i == 0 else ancestors_indexes[i - 1])
                if err:
                    return err
                parent_node.split_node(current_node, parent_index)
                _, err = self.dal.write_node(parent_node)
                if err:
                    return err
                current_node = parent_node
            else:
                break

        # Handle root split
        if current_node.is_over_populated() and current_node.page_num == self.root:
            new_root = self.dal.new_node([], [current_node.page_num])
            new_root.split_node(current_node, 0)
            new_root, err = self.dal.write_node(new_root)
            if err:
                return err
            self.root = new_root.page_num
            self.dal.root = self.root

        return None
    
    def Remove(self,key: bytes)-> None:
        try:
            root_node,err = self.dal.get_node(self.root)
        except Exception as e:
            raise RuntimeError(f"Error Retrieving roo node:{e}")
        
        try:
            remove_item_index,node_to_remove_from,ancestors_indexes,_ = root_node.find_key(key,True)
        except  Exception as e:
            raise RuntimeError(f"Error Finding Key:{e}")
        
        if remove_item_index == -1:
            return
        
        if node_to_remove_from.is_leaf():
            node_to_remove_from.remove_item_from_leaf(remove_item_index)
        else:
            try:
                affected_nodes = node_to_remove_from.remove_item_form_internal(remove_item_index)
                ancestors_indexes.extend(affected_nodes)
            except Exception as e:
                raise RuntimeError(f"Error Removig item from internal nod:{e}")
            
        try:
            ancestors,err = self.get_nodes(ancestors_indexes)
        except Exception as e:
            raise RuntimeError(f"Error Retrieving ancestor Node:{e}")
        
        for i in range(len(ancestors)-2,-1,-1):
            p_node = ancestors[i]
            node = ancestors[i+1]
            if node.is_under_populated():
                try:
                    p_node.rebalance_remove(node,ancestors_indexes[i+1])
                except Exception as e:
                    raise RuntimeError(f"Error Rebalancing node:{e}")
                
        root_node = ancestors[0]
        if len(root_node.items)==0 and len(root_node.child_nodes)>0:
            self.root = ancestors[1].page_num
             
                
        

    def get_nodes(self, indexes: List[int]) -> Tuple[List[Node], Optional[Exception]]:
        nodes = []
        current_node, err = self.dal.get_node(self.root)
        if err:
            return [], err
        nodes.append(current_node)

        for i in range(1, len(indexes)):
            current_node, err = self.dal.get_node(current_node.child_nodes[indexes[i]])
            if err:
                return [], err
            nodes.append(current_node)

        return nodes, None
    

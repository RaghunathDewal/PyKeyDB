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

        # Initialize root if it doesn't exist
        if self.root == 0:
            root, err = self.dal.write_node(self.dal.new_node([item], []))
            if err:
                return err
            self.root = root.page_num
            return None
        else:
            root, err = self.dal.get_node(self.root)
            if err:
                return err

        # Find the path to the node where the insertion should happen
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

        ancestors, err = self.get_nodes(ancestors_indexes)
        if err:
            return err

        # Rebalance the nodes all the way up
        for i in range(len(ancestors) - 2, -1, -1):
            pnode = ancestors[i]
            node = ancestors[i + 1]
            node_index = ancestors_indexes[i + 1]
            if node.is_over_populated():
                pnode.split(node, node_index)

        # Handle root
        root_node = ancestors[0]
        if root_node.is_over_populated():
            new_root = self.dal.new_node([], [root_node.page_num])
            new_root.split(root_node, 0)

            new_root, err = self.dal.write_node(new_root)
            if err:
                return err
            self.root = new_root.page_num

        return None

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
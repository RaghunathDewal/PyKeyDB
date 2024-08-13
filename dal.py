import os
from FList import FreeList
from meta import Meta, META_PAGE_NUM
from node import Node, Item
import const
from typing import List, Tuple, Optional

class Options:
    def __init__(self, page_size: int, min_fill_percent: float, max_fill_percent: float):
        self.page_size = page_size
        self.min_fill_percent = min_fill_percent
        self.max_fill_percent = max_fill_percent

default_Options = Options(
    page_size=4096,
    min_fill_percent=0.5,
    max_fill_percent=0.95
)

class Pgnum(int):
    pass

class Page:
    def __init__(self, num, data):
        self.num = Pgnum(num)
        self.data = data

class DAL:
    def __init__(self, file, options: Options):
        self.file = file
        self.page_size = options.page_size
        self.min_fill_percent = options.min_fill_percent
        self.max_fill_percent = options.max_fill_percent
        self.free_list = FreeList()
        self.meta = Meta()
        self.root = self.meta.root

    def max_threshold(self):
        return self.max_fill_percent * float(self.page_size)
    
    def is_over_populated(self, node):
        return float(node.node_size()) > self.max_threshold()
    
    def min_threshold(self):
        return self.min_fill_percent * float(self.page_size)
    
    def is_under_populated(self, node):
        return float(node.node_size()) < self.min_threshold
    
    def get_split_index(self, node):
        size = 0 
        size += const.NODE_HEADER_SIZE
        for i, item in enumerate(node.items):
            size += node.element_size(i)

            if float(size) > self.min_threshold() and i < len(node.items) - 1:
                return i + 1
        return -1
    
    def new_node(self, items: List[Item], child_nodes: List[Pgnum]) -> Node:
        node = Node.empty_node()
        node.items = items
        node.child_nodes = child_nodes
        node.page_num = self.get_nxt_page()
        node.dal = self
        return node

    def get_node(self, page_num):
        p, err = self.read_page(page_num)
        if err:
            print(f"Error reading page {page_num}: {err}")
            return None, err
        node = Node()
        try:
            node.deserialize(p.data)
        except Exception as e:
            print(f"Error deserializing node: {e}")
            return None, e
        node.page_num = page_num
        print(f"Node retrieved: Page Number {node.page_num}")
        return node, None
    
    def write_node(self, node):
        p = self.allocate_empty_page()
        if node.page_num == 0:
            p.num = self.get_nxt_page()
            node.page_num = p.num
            print(f"Assigning new page number {p.num} to node")
        else:
            p.num = node.page_num

        p.data = node.serialize(bytearray(self.page_size))

        err = self.write_page(p)
        if err:
            print(f"Error writing node to page {p.num}: {err}")
            return None, err
        print(f"Node written: Page Number {node.page_num}")
        return node, None
    
    def delete_node(self, page_num):
        self.release_page(page_num)

    @staticmethod
    def new_dal(path: str, options: Options) -> Tuple[Optional['DAL'], Optional[Exception]]:
        try:
            if os.path.exists(path):
                file = open(path, 'r+b')
                dal = DAL(file, options)
                
                meta, err = dal.ReadMeta()
                if err:
                    return None, err
                
                dal.meta = meta

                freelist, err = dal.Read_Freelist()
                if err:
                    return None, err
                
                dal.free_list = freelist

            else:
                file = open(path, 'w+b')
                dal = DAL(file, options)
                
                dal.free_list = FreeList()
                dal.meta.freelist_page = dal.get_nxt_page()

                _, err = dal.Write_Freelist()
                if err:
                    return None, err
                
                _, err = dal.WriteMeta(dal.meta)
                if err:
                    return None, err

                # Initialize the root node
                root_node = dal.new_node([], [])
                root_node, err = dal.write_node(root_node)
                if err:
                    return None, err
                
                dal.root = root_node.page_num
                dal.meta.root = dal.root  # Update the meta with the root page number
                _, err = dal.WriteMeta(dal.meta)  # Persist the meta with the new root
                if err:
                    return None, err

            return dal, None
        
        except IOError as e:
            return None, e
    
    def allocate_empty_page(self):
        print(f"Allocating new page: {self.free_list.get_nxt_page()}")
        return Page(0, bytearray(self.page_size))
    
    def read_page(self, page_num):
        p = self.allocate_empty_page()
        p.num = page_num

        offset = page_num * self.page_size

        self.file.seek(offset)
        p.data = self.file.read(self.page_size)

        if len(p.data) != self.page_size:
            return None, IOError("Could Not read full page")
        return p, None
    
    def write_page(self, p):
        offset = p.num * self.page_size
        self.file.seek(offset)
        self.file.write(p.data)
    
    def close(self):
        if self.file:
            try:
                self.file.close()
            except IOError as e:
                return f"could not close the file: {e}"
            self.file = None
        return None
    
    def get_nxt_page(self):
        return self.free_list.get_nxt_page()

    def release_page(self, page_num):
        self.free_list.release_page(page_num)

    def WriteMeta(self, meta):
        p = self.allocate_empty_page()
        p.num = META_PAGE_NUM
        buf = bytearray(self.page_size)
        p.data = meta.serialize(buf)

        err = self.write_page(p)
        if err:
            return None, err
        return p, None
    
    def ReadMeta(self):
        p, err = self.read_page(META_PAGE_NUM)
        if err:
            return None, err
        
        meta = Meta.empty_meta()
        meta.deserialize(p.data)
        return meta, None
    
    def Write_Freelist(self):
        p = self.allocate_empty_page()
        p.num = self.meta.freelist_page
        p.data = self.free_list.serialize()

        err = self.write_page(p)
        if err:
            return None, err
        return p, None

    def Read_Freelist(self):
        p, err = self.read_page(self.meta.freelist_page)
        if err:
            return None, err
        
        freelist = FreeList()
        freelist.deserialize(p.data)
        return freelist, None

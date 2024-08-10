import os
from FList import FreeList
from meta import Meta, META_PAGE_NUM
from node import Node

class Pgnum(int):
    pass

class Page:
    def __init__(self, num, data):
        self.num = Pgnum(num)
        self.data = data

class DAL:
    def __init__(self, file, page_size):
        self.file = file
        self.page_size = page_size
        self.free_list = FreeList()
        self.meta = Meta()
        self.root = self.meta.root



    def get_node(self,page_num):
        p, err = self.read_page(page_num)
        if err:
            return None, err
        node = Node()
        node.deserialize(p.data)
        node.page_num= page_num
        return node, None
    
    def write_node(self,node):
        node= Node()
        p = self.allocate_empty_page()
        if node.page_num == 0:
            p.num= self.get_nxt_page()
            node.page_num = p.num
        else:
            p.num = node.page_num

        p.data = node.Serailize(bytearray(self.page_size))

        err = self.write_page(p)
        if err:
            return None,err
        return node,None
    
    def delete_node(self,page_num):
        self.release_page(page_num)


    @staticmethod
    def new_dal(path, page_size):
        try:
            if os.path.exists(path):
                file = open(path, 'r+b')
                dal = DAL(file, page_size)
                
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
                dal = DAL(file, page_size)
                
                dal.free_list = FreeList()
                dal.meta.freelist_page = dal.get_nxt_page()
                
                _, err = dal.Write_Freelist()
                if err:
                    return None, err
                
                _, err = dal.WriteMeta(dal.meta)
                if err:
                    return None, err

            return dal, None
        
        except IOError as e:
            return None, e
    
    def allocate_empty_page(self):
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
        p.data = meta.Serialize()

        err = self.write_page(p)
        if err:
            return None, err
        return p, None
    
    def ReadMeta(self):
        p, err = self.read_page(META_PAGE_NUM)
        if err:
            return None, err
        
        meta = Meta.empty_meta()
        meta.deSerialize(p.data)
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

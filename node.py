import struct
from typing import Tuple ,Optional
class Item:
    def __init__(self,key = bytes,value= bytes):
        self.key = key
        self.value= value


class Node:
    PAGE_NUM_SIZE = 8
    def __init__(self,dal = None, page_num=None):
        self.dal=dal
        self.page_num= 0
        self.items=[]
        self.child_nodes= []



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
                return True,1
            
            if res ==1:
                return False,i
            
        return False,len(self.items)
    
    def find_key_hepler(self,key:bytes)-> Tuple[int, Optional["Node"],Optional[Exception]]:
        was_found,index = self.find_key_in_node(key)
        if was_found:
            return index,self,None
        
        if self.is_leaf():
            return -1, None,None
        
        try:
            next_child =self.get_node(self.child_nodes[index])
        except Exception as e:
            return -1,None,e
        
        return next_child.find_key_helper(key)
    
    def find_key(self, key:bytes)-> Tuple[int, Optional["Node"],Optional[Exception]]:
        index, node, err=self.find_key_hepler(key)
        if err:
            return -1 ,None,err
        return index,node,None
       

    @staticmethod
    def empty_node():
        return Node()
    
    @staticmethod
    def new_item(key : bytes, value: bytes):
        return Item(key,value)
    
    def is_leaf(self):
        return len(self.child_nodes)==0
    
    def Serailize(self,buf : bytearray) -> bytearray:
        left_pos = 0
        right_pos= len(buf) -1

        is_leaf = 1 if self.is_leaf else 0
        buf[left_pos] = is_leaf
        left_pos +=1


        struct.pack_into('<H',buf,left_pos,len(self.items))
        left_pos+=2

        for i,item in enumerate(self.items):
            if not self.is_leaf():
                child_node =self.child_nodes[i]
                struct.pack_into('<H',buf,left_pos,child_node)
                left_pos += self.PAGE_NUM_SIZE
            
            klen= len(item.key)
            vlen =len(item.value)

            offset =  right_pos - vlen - klen - 2
            struct.pack_into('<H',buf,offset)
            left_pos += 2

            right_pos -= vlen
            buf[right_pos:right_pos+vlen] =item.value

            right_pos -= 1
            buf[right_pos] = vlen 

            right_pos -= klen
            buf[right_pos:right_pos+klen]= item.key

            right_pos -= 1 
            buf[right_pos] = klen
            
        if not self.is_leaf():
            last_child_node =  self.child_nodes[-1]
            struct.pack_into('<Q',buf,left_pos,last_child_node)

        return buf
    
    def deserialize(self, buf):
        left_pos = 0

        is_leaf = buf[0]
        items_count = struct.unpack_from('<H', buf, 1)[0]
        left_pos += 3

        for i in range(items_count):
            if is_leaf == 0:
                page_num = struct.unpack_from('<Q', buf, left_pos)[0]
                left_pos += self.PAGE_NUM_SIZE
                self.child_nodes.append(page_num)

            offset = struct.unpack_from('<H', buf, left_pos)[0]
            left_pos += 2

            klen = buf[offset]
            offset += 1

            key = buf[offset:offset + klen]
            offset += klen

            vlen = buf[offset]
            offset += 1

            value = buf[offset:offset + vlen]
            offset += vlen
            self.items.append(Item(key, value))

        if is_leaf == 0:
            page_num = struct.unpack_from('<Q', buf, left_pos)[0]
            self.child_nodes.append(page_num)










   
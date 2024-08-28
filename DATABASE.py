import os
import threading
import const
from dal import DAL, Options
from FList import FreeList
from typing import Optional

class DB():
    def __init__(self, dal: Optional['DAL'] = None):
        self.rwlock = threading.RLock()
        self.dal = dal
        self.freelist = FreeList()
        self.root = dal.meta.root if dal and dal.meta else 0

    @classmethod
    def open(cls, path, options: Options):
        options.page_size = const.PAGE_SIZE
        dal, err = DAL.new_dal(path, options)
        if dal is None or err:
            raise Exception(f"Error initializing DAL: {err}")
        db = cls(dal)
        print(f"Database opened. Root page number: {db.root}")
        return db
    
    def close(self):
        return self.dal.close()
    
    def read_tx(self):
        self.rwlock.acquire(blocking=True, timeout=-1)
        return self.new_tx(False)
    
    def write_tx(self):
        self.rwlock.acquire(blocking=True, timeout=-1)
        return self.new_tx(True)
    
    def new_tx(self, writable):
        from Txn import Tx
        return Tx(self, writable)
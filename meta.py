import struct

META_PAGE_NUM = 0

class Meta:
    PAGE_NUM_SIZE = 8

    def __init__(self, root: int = 0, freelist_page: int = 0):
        self.root = root
        self.freelist_page = freelist_page

    @staticmethod
    def empty_meta():
        return Meta()

    def serialize(self, buf: bytearray) -> bytearray:
        pos = 0
        struct.pack_into('<Q', buf, pos, self.root)
        pos += self.PAGE_NUM_SIZE

        struct.pack_into('<Q', buf, pos, self.freelist_page)
        pos += self.PAGE_NUM_SIZE

        return buf
    
    def deserialize(self, buf: bytearray):
        pos = 0

        self.root = struct.unpack_from('<Q', buf, pos)[0]
        pos += self.PAGE_NUM_SIZE

        self.freelist_page = struct.unpack_from('<Q', buf, pos)[0]
        pos += self.PAGE_NUM_SIZE

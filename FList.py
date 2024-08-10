import struct

class FreeList:
    PAGE_NUM_SIZE = 8

    def __init__(self):
        self.max_pg = 0
        self.released_pages = []

    def serialize(self):
        buf = bytearray()
        buf.extend(struct.pack('<H', self.max_pg))
        buf.extend(struct.pack('<H', len(self.released_pages)))
        for page in self.released_pages:
            buf.extend(struct.pack('<Q', page))
        return buf
    
    def deserialize(self, buf):
        self.max_pg, = struct.unpack_from('<H', buf, 0)
        pos = 2
        released_pages_count, = struct.unpack_from('<H', buf, pos)
        pos += 2
        self.released_pages = []
        for _ in range(released_pages_count):
            page, = struct.unpack_from('<Q', buf, pos)
            self.released_pages.append(page)
            pos += self.PAGE_NUM_SIZE

    def get_nxt_page(self):
        if self.released_pages:
            return self.released_pages.pop()
        self.max_pg += 1
        return self.max_pg

    def release_page(self, page):
        self.released_pages.append(page)
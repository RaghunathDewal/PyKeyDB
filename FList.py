import struct

class FreeList:
    PAGE_NUM_SIZE = 8  # Same as in Meta class for consistency

    def __init__(self):
        self.max_pg = 0
        self.released_pages = []

    def serialize(self):
        buf = bytearray()
        buf.extend(struct.pack('<I', self.max_pg))  # Use 4-byte integer for max_pg
        buf.extend(struct.pack('<I', len(self.released_pages)))  # 4-byte integer for count
        for page in self.released_pages:
            buf.extend(struct.pack('<Q', page))
        return buf
    
    def deserialize(self, buf):
        self.max_pg, = struct.unpack_from('<I', buf, 0)
        pos = 4  # Move to next position
        released_pages_count, = struct.unpack_from('<I', buf, pos)
        pos += 4
        self.released_pages = []
        for _ in range(released_pages_count):
            page, = struct.unpack_from('<Q', buf, pos)
            self.released_pages.append(page)
            pos += self.PAGE_NUM_SIZE

    def get_nxt_page(self):
        if self.released_pages:
            next_page = self.released_pages.pop()
            print(f"Reusing released page: {next_page}")
            return next_page
        self.max_pg += 1
        print(f"Allocating new page: {self.max_pg}")
        return self.max_pg

    def release_page(self, page):
        self.released_pages.append(page)

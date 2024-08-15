import struct
import const

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
        struct.pack_into('<I', buf, pos, const.MAGIC_NUMBER)  # Explicitly write the magic number
        pos += const.MAGIC_NUMBER_SIZE

        struct.pack_into('<Q', buf, pos, self.root)
        pos += const.PAGE_NUM_SIZE

        struct.pack_into('<Q', buf, pos, self.freelist_page)
        pos += const.PAGE_NUM_SIZE

        return buf 

    def deserialize(self, buf: bytes) -> None:
        pos = 0
        magic_number_res = struct.unpack_from('<I', buf, pos)[0]  # unpack uint32 in little-endian
        pos += const.MAGIC_NUMBER_SIZE

        if magic_number_res != const.MAGIC_NUMBER:
            raise ValueError(f"The file is not a RD db file (invalid magic number: {magic_number_res})")

        self.root = struct.unpack_from('<Q', buf, pos)[0]  # unpack uint64 in little-endian
        pos += const.PAGE_NUM_SIZE

        self.freelist_page = struct.unpack_from('<Q', buf, pos)[0]  # unpack uint64 in little-endian
        pos += const.PAGE_NUM_SIZE

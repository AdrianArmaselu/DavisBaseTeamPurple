import os
import struct


class Page:
    page_size = 512
    datePattern = "yyyy-MM-dd_HH:mm:ss"

    def __init__(self):
        pass

    def get_root_node(self, table_file_path):
        if os.stat(table_file_path).st_size == 0:
            table_root_node = [1, 0]
            with open(table_file_path, "wb") as fh:
                root_node_size = len(table_root_node)
                root_node = struct.pack('i' * root_node_size, *table_root_node)
                fh.write(root_node)
        else:
            table_root_node = []

            with open(table_file_path, 'rb') as f:
                f.seek(0, 0)
                i = 0
                node_value = f.read(4)
                while node_value != b'\x00\x00\x00\x00' and i < self.page_size:
                    table_root_node.append(struct.unpack('i', node_value)[0])
                    node_value = f.read(4)
                    i += 4
        return table_root_node

    def check_page_size(self, table_file_path, page_number):

        with open(table_file_path, 'rb') as f:
            f.seek(page_number * self.page_size + 1, 0)
            page_size = 0
            while page_size < 512:
                bytes1 = f.read(1)
                if bytes1 == b'':
                    break
                page_size += 1
        return page_size

    def write_to_page(self, table_file_path, page_number, start_byte, record_values, fstring, record_payload=0):

        with open(table_file_path, "r+b") as fh:
            page_offset = start_byte
            fh.seek(page_offset, 0)
            record = struct.pack(fstring, *record_values)
            fh.write(record)
            return True
        return False

    def read_page(self, table_file_path, column_dtype, page_number, record_fstring):
        fstring_value = {"x": 0, "h": 2, "i": 4, "q": 8, "f": 4, "d": 8, "Q": 8, "B": 1, "b": 1, "H": 2, "s": "1"}
        with open(table_file_path, 'rb') as fh:
            page_offset = page_number * self.page_size + 1
            page_end = page_offset + self.page_size
            for f_str in record_fstring:
                fh.seek(page_offset, 0)
                read_bytes = fstring_value[f_str]
                if f_str != "s":
                    rec_value = fh.read(read_bytes)
                else:
                    pass
                page_offset += read_bytes


    def update_root_node(self, table_file_path, updated_root):
        with open(table_file_path, "r+b") as fh:
            fh.seek(0, 0)
            root_node_size = len(updated_root)
            root_node = struct.pack('i' * root_node_size, *updated_root)
            fh.write(root_node)

    def bplus_tree_split_page(self, tbl_root_node):
        pass

from typing import AnyStr, NewType, Tuple, List

from constants import TABLE_BTREE_LEAF_PAGE, INDEX_BTREE_INTERIOR_PAGE, INDEX_BTREE_LEAF_PAGE
from util import bytes_to_int, int_to_bytes, log_debug

PageHeader = Tuple[int, int, int, int, int, List[int]]


def get_column_size(data_type: int) -> int:
    if data_type < 12:
        return {
            0: 0,
            1: 1,
            2: 2,
            3: 4,
            4: 8,
            5: 4,
            6: 8,
            8: 1,
            9: 4,
            10: 8,
            11: 8
        }[data_type]
    else:
        return data_type


# READ tests file for a demo on how to use this class
# Returns: array of pages, where each page is a tuple containing:
# (page_type:int, page_parent:int, node_page_number:int, cells:array of tuples)
# cells: array of (row_id:int, columns:array of byte[], each byte[] being the value, data_types:int[]) if leaf
# cells: array of (row_id, left_child_page_number) if interior node

class TableFile:

    # mode can be rb for reading binary, or wb for writing binary, respectively
    def __init__(self, table_name: str, mode: str):
        self.table_name = table_name
        self.table_file = open(table_name, mode)

    # function wrappers
    def _read(self, n: int) -> AnyStr:
        return self.table_file.read(n)

    def _write(self, s: AnyStr) -> int:
        return self.table_file.write(s)

    def tell(self) -> int:
        return self.table_file.tell()

    def seek(self, n: int, whence: int = 0) -> int:
        return self.table_file.seek(n, whence)

    def skip(self, n: int) -> int:
        return self.table_file.seek(n, 1)

    # domain io functions
    def read_int(self, size: int) -> int:
        return bytes_to_int(self._read(size))

    def backtrack_write(self, byte_array: AnyStr, size=-1):
        if size < 0:
            size = len(byte_array)
        self.seek(-size, 1)
        self._write(byte_array)
        self.seek(-size, 1)
        log_debug('backtrack_write', byte_array, byte_array.decode('utf-8'), size, self.tell())

    def does_page_exist_at_position(self, pos: int) -> bool:
        self.table_file.seek(pos)
        return self.read_int(1) in [2, 5, 10, 13]

    def read_page_header(self) -> PageHeader:
        log_debug("reading page header...")

        page_type = self.read_int(1)
        log_debug('read page_type', page_type)

        self.skip(1)

        number_of_cells = self.read_int(2)
        log_debug('read number_of_cells', number_of_cells)

        content_area_offset = self.read_int(2)
        log_debug('read content_area_offset', content_area_offset)

        page_number = self.read_int(4)
        log_debug('read page_number', page_number)

        page_parent = self.read_int(4)
        log_debug('read page_parent', page_parent)

        self.skip(2)

        cell_offsets = [self.read_int(2) for c in range(number_of_cells)]
        log_debug('read', len(cell_offsets), 'cell offsets')

        log_debug('finished reading page header.')

        return page_type, number_of_cells, content_area_offset, page_number, page_parent, cell_offsets

    def read_column(self, data_type: int) -> AnyStr:
        column_size = get_column_size(data_type)
        return self.table_file.read(column_size)

    def read_interior_cell(self):
        left_child_page_number = self.read_int(4)
        row_id = self.read_int(4)
        return row_id, left_child_page_number

    def read_leaf_cell(self):
        # header
        payload_size = self.read_int(2)  # not really useful for us, unless we do optimizations
        row_id = self.read_int(4)
        # body
        number_of_columns = self.read_int(1)
        data_types = [self.read_int(1) for c in range(number_of_columns)]
        columns = [self.read_column(data_type) for data_type in data_types]
        return row_id, columns, data_types

    def read(self):
        page_position = 0
        pages = []
        while self.does_page_exist_at_position(page_position):
            self.seek(page_position)
            page_type, _, content_area_offset, node_page_number, page_parent, cell_offsets = self.read_page_header()
            self.seek(page_position + content_area_offset)
            cells = [self.read_leaf_cell() for offset in cell_offsets] if page_type == TABLE_BTREE_LEAF_PAGE else \
                [self.read_interior_cell() for offset in cell_offsets]
            # todo transform cells to values based on types
            pages.append((page_type, page_parent, node_page_number, cells))
            page_position += 512
        self.table_file.close()
        return pages

    # assuming each column is array of bytes
    def write_column(self, column, data_type):
        data_type_size = get_column_size(data_type)
        column_bytes = bytes(column, 'utf-8') if isinstance(column, str) else \
            int_to_bytes(column, data_type_size)
        # fill empty bytes
        if len(column_bytes) != data_type_size:
            self.backtrack_write(bytes(data_type_size - len(column_bytes)))
        self.backtrack_write(column_bytes)

    def write(self, data):
        page_position = 0
        for page_type, page_parent, page_number, cells in data:
            self.seek(page_position + 512)
            cell_offsets = []
            last_cell_position = page_position + 512

            # write records
            for row_id, columns, types in cells:
                if page_type == TABLE_BTREE_LEAF_PAGE:
                    [self.write_column(column=columns[i], data_type=types[i]) for i in range(len(columns))]
                    # writing data types
                    # assuming types is an array of 1-byte ints
                    self.backtrack_write(bytearray(types))

                    # writing number of columns
                    self.backtrack_write(int_to_bytes(len(columns), 1))
                    cell_offsets.append(int_to_bytes(self.tell() % 512, 2))

                    # writing row id
                    self.backtrack_write(int_to_bytes(row_id, 4))

                    # writing payload size
                    self.backtrack_write(int_to_bytes(last_cell_position - self.tell() - 4, 2))

                    last_cell_position = self.tell()
                else:
                    self.backtrack_write(int_to_bytes(row_id, 4))
                    self.backtrack_write(int_to_bytes(page_parent, 4))

            # write the header
            content_offset = self.tell()
            self.seek(page_position)
            self._write(int_to_bytes(page_type, 1))
            self.skip(1)
            self._write(int_to_bytes(len(cells), 2))
            self._write(int_to_bytes(content_offset, 2))
            self._write(int_to_bytes(page_number, 4))
            self._write(int_to_bytes(page_parent, 4))
            self.skip(2)
            for offset in cell_offsets:
                self._write(offset)
            page_position += 512


class IndexFile(TableFile):
    def __init__(self, table_name):
        super(IndexFile, self).__init__(table_name)

    def read_leaf_cell(self):
        # header
        payload_size = self.read_int(2)

        # payload
        number_of_rowids = self.read_int(1)
        index_data_type = self.read_int(1)
        index_value = self._read(get_column_size(index_data_type))
        row_ids = [self.read_int(4) for _ in range(number_of_rowids)]

        return index_data_type, -1, index_value, row_ids

    def read_interior_cell(self):
        # header
        left_child_page_number = self.read_int(4)
        payload_size = self.read_int(2)

        # payload
        number_of_rowids = self.read_int(1)
        index_data_type = self.read_int(1)
        index_value = self._read(get_column_size(index_data_type))
        row_ids = [self.read_int(4) for _ in range(number_of_rowids)]
        return index_data_type, left_child_page_number, index_value, row_ids

    def read(self):
        page_position = 0
        pages = []
        while self.does_page_exist_at_position(page_position):
            self.seek(page_position)
            page_type, _, content_area_offset, node_page_number, page_parent, cell_offsets = self.read_page_header()
            self.seek(page_position + content_area_offset)
            cells = [self.read_leaf_cell() for offset in cell_offsets] if page_type == INDEX_BTREE_LEAF_PAGE else \
                [self.read_interior_cell() for offset in cell_offsets]
            # todo transform cells to values based on types
            pages.append((page_type, page_parent, node_page_number, cells))
            page_position += 512
        self.table_file.close()
        return pages


    def write(self, data):
        page_position = 0
        for page_type, page_parent, page_number, cells in data:
            self.seek(page_position + 512)
            cell_offsets = []
            last_cell_position = page_position + 512

            # write records
            for index_data_type, left_child_page_number, index_value, row_ids in cells:
                # writing row ids
                [self.backtrack_write(int_to_bytes(row_id, 4)) for row_id in range(len(row_ids))]

                # index value
                self.backtrack_write(int_to_bytes(index_value, get_column_size(index_data_type)))

                # index data type
                self.backtrack_write(int_to_bytes(index_data_type, 1))

                # number of row ids
                self.backtrack_write(int_to_bytes(len(row_ids), 1))

                # number of bytes of cell payload
                self.backtrack_write(int_to_bytes(last_cell_position - self.tell(), 2))

                last_cell_position = self.tell()
                # left child page
                if page_type == TABLE_BTREE_LEAF_PAGE:
                    self.backtrack_write(int_to_bytes(left_child_page_number, 4))

            # write the header
            content_offset = self.tell()
            self.seek(page_position)
            self._write(int_to_bytes(page_type, 1))
            self.skip(1)
            self._write(int_to_bytes(len(cells), 2))
            self._write(int_to_bytes(content_offset, 2))
            self._write(int_to_bytes(page_number, 4))
            self._write(int_to_bytes(page_parent, 4))
            self.skip(2)
            for offset in cell_offsets:
                self._write(offset)
            page_position += 512

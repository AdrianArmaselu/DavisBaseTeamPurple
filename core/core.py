from typing import AnyStr, List, Dict, Generic, TypeVar
from io import BytesIO
import os
import math

# Constants
INDEX_BTREE_INTERIOR_PAGE = 2
INDEX_BTREE_LEAF_PAGE = 10

# Constants
TABLE_BTREE_INTERIOR_PAGE = 5
TABLE_BTREE_LEAF_PAGE = 13

data_types = {
    0: "NULL",
    1: "TINYINT",
    2: "SMALLINT",
    3: "INT",
    4: "BIGINT",
    5: "FLOAT",
    6: "DOUBLE",
    8: "YEAR",
    9: "TIME",
    10: "DATETIME",
    11: "DATE",
    12: "TEXT",
}

data_type_encodings = {v: k for k, v in data_types.items()}


def flatten(input_list: List) -> List:
    return [item for sublist in input_list for item in sublist]


def bytes_to_int(number_bytes: AnyStr) -> int:
    return int.from_bytes(number_bytes, 'big')


def int_to_bytes(number: int, size: int = 4) -> AnyStr:
    return int.to_bytes(number, size, 'big')


def get_column_size(column_type: int) -> int:
    return {0: 0, 1: 1, 2: 2, 3: 4, 4: 8, 5: 4, 6: 8, 8: 1, 9: 4, 10: 8, 11: 8}[column_type] \
        if column_type < 12 else column_type


# Abstract Class
class DavisBaseSerializable:
    def as_bytes(self) -> AnyStr:
        pass

    def from_bytes(self):
        pass


class Condition:
    def __init__(self, column_name: str, operator: str, value: str):
        self.column_name: str = column_name
        self.operator: str = operator
        self.value: str = value

    def is_satisfied(self, value: str):
        return {
            "=": lambda a, b: a == b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b
        }[self.operator](value, self.value)


class TableColumnsMetadata:
    def __init__(self):
        self.columns = {}


class CreateArgs:
    def __init__(self, columns_metadata: TableColumnsMetadata):
        self.columns_metadata: TableColumnsMetadata = columns_metadata


class InsertArgs:
    def __init__(self, record: List):
        self.record: List = record


class UpdateArgs:
    def __init__(self, column_name: str, value, condition):
        self.column_name: str = column_name
        self.value = value
        self.condition = condition


class DeleteArgs:
    def __init__(self):
        pass


class SelectArgs:
    def __init__(self, column_names: List[str], condition: Condition):
        self.column_names: List[str] = column_names
        self.condition: Condition = condition


def value_to_bytes(value: str or int, value_byte_size: int) -> AnyStr:
    return bytes(value, 'utf-8') if isinstance(value, str) else int_to_bytes(value, value_byte_size)


def bytes_to_value(value_bytes: AnyStr, column_type: int) -> str or int:
    return bytes_to_int(value_bytes) if column_type < 12 else str(value_bytes)


def record_payload_size(record_data_types: List[int]) -> int:
    return sum([get_column_size(data_type) for data_type in record_data_types])


def leaf_cell_header_size():
    return 2 + 4


class Record(DavisBaseSerializable):
    def __init__(self, record_data_types: List[int], values: List[str or int] = None):
        if values is None:
            values = []
        self.data_types: List[int] = record_data_types
        self.values: List[str or int] = values

    def size(self) -> int:
        return self.header_size() + self.body_size()

    def header_size(self) -> int:
        return 1 + self.number_of_columns()

    def body_size(self) -> int:
        return record_payload_size(self.data_types)

    def number_of_columns(self) -> int:
        return len(self.data_types)

    def as_bytes(self) -> AnyStr:
        return self.header_bytes() + self.payload()

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(self.number_of_columns(), 1) + bytes(self.data_types)

    def payload(self) -> AnyStr:
        return b''.join([
            value_to_bytes(self.values[i], get_column_size(self.data_types[i]))
            for i in range(self.number_of_columns())
        ])


class PageCell(DavisBaseSerializable):
    def __init__(self, row_id: int):
        self.row_id = row_id


class InternalCell(PageCell):
    def __init__(self, row_id: int, left_child_page: int):
        super(InternalCell, self).__init__(row_id)
        self.left_child_page: int = left_child_page

    def as_bytes(self) -> AnyStr:
        return self.header_bytes()

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(self.left_child_page) + int_to_bytes(self.row_id)


class LeafCell(PageCell):
    def __init__(self, row_id: int, record: Record = None):
        super(LeafCell, self).__init__(row_id)
        self.record: Record = record

    def as_bytes(self) -> AnyStr:
        return self.header_bytes() + self.payload()

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(self.record.size(), 2) + int_to_bytes(self.row_id)

    def payload(self) -> AnyStr:
        return self.record.as_bytes()


class TablePage(DavisBaseSerializable):
    def __init__(self, page_number: int, page_parent: int, data_types: List[int or str] = None, cells: Dict = None):
        self.page_number: int = page_number
        self.page_parent: int = page_parent
        self.data_types = data_types
        if cells is None:
            cells = {}
        self.cells: Dict = cells

    # abstract function
    def insert_record(self, row_id: int, args: InsertArgs):
        pass

    # abstract function
    def is_full(self):
        pass

    def remove_record(self, row_id):
        pass

    def __str__(self):
        return 'test'


class TableLeafPage(TablePage):
    PAGE_TYPE = 13

    def insert_record(self, row_id: int, args: InsertArgs):
        self.cells[row_id] = LeafCell(row_id, Record(self.data_types, args.record))

    def add_cell(self, row_id: int, cell: LeafCell = None):
        self.cells[row_id] = cell

    def is_full(self, with_cell: bool = False):
        size = self.header_size() + self.payload_size()
        return with_cell and size + self.leaf_cell_size() >= 512 or size >= 512

    def header_size(self) -> int:
        return 13 + 2 * len(self.cells)

    def payload_size(self) -> int:
        return self.leaf_cell_size() * len(self.cells)

    def leaf_cell_size(self) -> int:
        return leaf_cell_header_size() + record_payload_size(self.data_types)

    def as_bytes(self) -> AnyStr:
        return self.header_bytes() \
               + bytes([0 for i in range(512 - self.header_size() - self.payload_size())]) \
               + self.payload()

    def header_bytes(self) -> AnyStr:
        return b''.join([
            int_to_bytes(self.PAGE_TYPE, 1),
            int_to_bytes(len(self.cells), 2),
            int_to_bytes(512 - self.payload_size(), 2),
            int_to_bytes(self.page_number),
            int_to_bytes(self.page_parent),
            self.cell_locations_bytes()])

    def cell_locations_bytes(self) -> AnyStr:
        return b''.join([int_to_bytes(512 - i * (Record(self.data_types).size()), 2) for i in range(len(self.cells))])

    def payload(self) -> AnyStr:
        return b''.join([self.cells[row_id].as_bytes() for row_id in self.cells])

    def __str__(self):
        return str(self.cells)


# Abstract class
class AbstractTableModel:
    def as_list(self) -> List[TablePage]:
        pass

    def insert(self, row_id: int, args: InsertArgs):
        pass

    def update(self, args: UpdateArgs):
        pass

    def delete(self, args):
        pass

    def select(self, args):
        pass


class ListTableModel(AbstractTableModel):
    def __init__(self):
        self.pages: List[TablePage] = []
        self.current_page: int = 0 if len(self.pages) == 0 else len(self.pages) - 1

    def as_list(self) -> List[TablePage]:
        return self.pages

    def insert(self, row_id: int, args: InsertArgs):
        if self.pages[self.current_page].is_full():  # is full or is above full with the new element
            self.pages.append(TableLeafPage(self.current_page + 1, 0))
            self.current_page += 1
        self.pages[self.current_page].insert_record(row_id, args)

    def update(self, args: UpdateArgs):
        pass

    def select(self, args: SelectArgs):
        #
        # rows = []
        # for page in self.pages:
        #     for cell in page.cells:
        #         leaf_cell: TableLeafCell = cell
        #         args.condition.is_satisfied(leaf_cell.columns.)
        pass

    def delete(self, args: DeleteArgs):
        pass

    def __str__(self) -> str:
        return str(self.pages)


class DavisTable(DavisBaseSerializable):
    def __init__(self, columns_metadata: TableColumnsMetadata):
        self.model: AbstractTableModel = ListTableModel()
        self.current_row_id: int = 0
        self.columns_metadata: TableColumnsMetadata = columns_metadata

    def insert(self, args: InsertArgs):
        self.model.insert(self.current_row_id, args)
        self.current_row_id += 1

    def update(self, args: UpdateArgs):
        self.model.update(args)

    def select(self, args: SelectArgs):
        self.model.select(args)

    def delete(self, args: DeleteArgs):
        self.model.delete(args)

    def as_bytes(self) -> bytes:
        content = b''
        for page in self.model.as_list():
            content += page.as_bytes()
        return content

    def __str__(self) -> str:
        return str(self.model)


class DavisIndex(DavisBaseSerializable):
    def __init__(self, name):
        self.name = name


class PageReader:
    def __init__(self, page_bytes):
        self.page_bytes = page_bytes
        self.reader = BytesIO(self.page_bytes)

    def read(self, size: int) -> bytes:
        return self.reader.read(size)

    def seek(self, n: int, whence: int = 0) -> int:
        return self.reader.seek(n, whence)

    def tell(self):
        return self.tell()

    def read_int(self, size: int = 4) -> int:
        return bytes_to_int(self.reader.read(size))

    def skip(self, n: int) -> int:
        return self.reader.seek(n, 1)

    def read_byte(self) -> int:
        return self.read_int(1)

    def read_short(self) -> int:
        return self.read_int(2)

    def read_page(self) -> TablePage:
        page_type = self.read_byte()
        number_of_cells = self.read_short()
        content_area_offset = self.read_short()
        page_number = self.read_int()
        page_parent = self.read_int()
        cells_offsets = self.read(2 * number_of_cells)
        page = TableLeafPage(page_number=page_number, page_parent=page_parent)
        for cell_offset in cells_offsets:
            self.seek(cell_offset)
            if page_type == TABLE_BTREE_LEAF_PAGE:
                cell_payload_size = self.read_short()
                row_id = self.read_int()
                column_data_types = self.read(self.read_byte())
                page.data_types = column_data_types
                values = [bytes_to_value(self.read(get_column_size(column_type)), column_type) for column_type in
                          column_data_types]
                page.add_cell(row_id, LeafCell(row_id, Record(data_types, values)))
            if page_type == TABLE_BTREE_INTERIOR_PAGE:
                left_child_page_number = self.read_int()
                row_id = self.read_int()
                # table.add_cell(row_id, LeafCell(row_id, Record(data_types, values)))
        return page


class TableFile:

    def __init__(self, path: str):
        self.path = path
        self.table_file = None
        self.file_size = os.path.getsize(self.path)

    def read_pages(self) -> List[TablePage]:
        self.table_file = open(self.path, "rb")
        pages = [self.read_page() for page in range(math.ceil(self.file_size / 512))]
        self.close()
        return pages

    def write(self, table: DavisTable):
        self.table_file = open(self.path, "wb")
        self.table_file.write(table.as_bytes())
        self.table_file.close()

    def read_page(self) -> TablePage:
        return PageReader(self.table_file.read(512)).read_page()

    def close(self):
        self.table_file.close()


def create_path_if_not_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path)


THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(THIS_FOLDER, 'myfile.txt')


class DavisBaseFS:
    CATALOG_FOLDER_PATH = 'catalog'
    DATA_FOLDER_PATH = 'storage'

    def __init__(self, folder: str):
        self.folder: str = os.path.abspath(folder)
        create_path_if_not_exists(self.catalog_folder_path())
        create_path_if_not_exists(self.storage_folder_path())

    def catalog_folder_path(self) -> str:
        return self.folder + '/' + self.CATALOG_FOLDER_PATH

    def storage_folder_path(self) -> str:
        return self.folder + "/" + self.DATA_FOLDER_PATH

    def read_catalog_table(self, name):
        path = self.catalog_folder_path() + '/' + name + ".tbl"
        if os.path.isfile(path):
            pages = TableFile(os.path.abspath(path)).read_pages()
            table = DavisTable()
            # table.model.
        return None

    def read_tables_table(self) -> DavisTable:
        return self.read_catalog_table('davisbase_table')

    def write_tables_table(self, table: DavisTable):
        return self.write_catalog_table(table)

    def read_columns_table(self) -> DavisTable:
        return self.read_catalog_table('davisbase_columns')

    def write_columns_table(self, table: DavisTable):
        return self.write_catalog_table(table)

    def get_table(self, name: str):
        pass

    def write_data_table(self, table: DavisTable):
        path = self.storage_folder_path() + '/' + table.name + ".tbl"
        self.write_table(path, table)

    def write_catalog_table(self, table: DavisTable):
        path = self.catalog_folder_path() + '/' + table.name + ".tbl"
        self.write_table(path, table)

    def write_table(self, path: str, table: DavisTable):
        with open(path, "wb") as table_file:
            table_file.write(table.as_bytes())
            table_file.close()

    def write_index(self, index: DavisIndex):
        pass


class DavisBase:
    def __init__(self):
        self.tables: Dict[str, DavisTable] = {}
        self.indexes = {}
        self.fs = DavisBaseFS('../data')
        self.tables_table = self.fs.read_tables_table()
        self.columns_table = self.fs.read_columns_table()

        if not self.tables_table:
            self.tables_table = self.create_table()

        if self.tables_table.current_row_id == 0:
            self.tables_table.insert(InsertArgs(record=[3, 3]))

    def show_tables(self) -> List[str]:
        return [table_name for table_name in self.tables]

    def create_table(self) -> DavisTable:
        return DavisTable()

    def drop_table(self):
        # delete table
        # update metadata
        pass

    def create_index(self):
        # Index_Btree(self,5)
        pass

    def insert(self, table_name: str, args: InsertArgs):
        self.tables[table_name].insert(args)

    def update(self, table_name: str, args: UpdateArgs):
        self.tables[table_name].update(args)

    def select(self, table_name: str, args: SelectArgs):
        self.tables[table_name].select(args)

    def delete(self, table_name: str, args: DeleteArgs):
        self.tables[table_name].delete(args)

    def commit(self):
        self.fs.write_tables_table(self.tables_table)
        self.fs.write_tables_table(self.columns_table)
        for table_name in self.tables:
            self.fs.write_data_table(self.tables[table_name])
        for index_name in self.indexes:
            self.fs.write_index(self.indexes[index_name])
        # [open(index.name + '.ndx', 'wb').write(index.to_binary()) for index in self.indexes]


# davis_base = DavisBase()
# davis_base.commit()
rows = {1: "a", 2: "b"}
for row in rows:
    print(row)

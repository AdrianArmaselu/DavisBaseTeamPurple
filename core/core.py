from typing import AnyStr, List, Dict, Generic, TypeVar
from io import BytesIO
import os
import math

# Constants
from util import log_debug

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
        if column_type < 12 else column_type - 12


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
    def __init__(self, columns=None):
        if columns is None:
            columns = {}
        self.columns: Dict = columns

    def data_types(self) -> List[int]:
        return [data_type_encodings[column_type_index] for column_name, column_type_index in self.columns.items()]


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
    def __init__(self, column_names: List[str], condition: Condition = None):
        self.column_names: List[str] = column_names
        self.condition: Condition = condition


def value_to_bytes(value: str or int, value_byte_size: int) -> AnyStr:
    return bytes(value, 'utf-8') if isinstance(value, str) else int_to_bytes(value, value_byte_size)


def bytes_to_value(value_bytes: AnyStr, column_type: int) -> str or int:
    return bytes_to_int(value_bytes) if column_type < 12 else value_bytes.decode("utf-8")


def record_payload_size(record_data_types: List[int]) -> int:
    return sum([get_column_size(data_type) for data_type in record_data_types])


def leaf_cell_header_size():
    return 2 + 4


class Record(DavisBaseSerializable):
    def __init__(self, record_data_types: List[int], values: List[str or int]):
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
        # log_debug("record bytes", self.header_bytes(), self.payload(), self.header_bytes() + self.payload())
        return self.header_bytes() + self.payload()

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(self.number_of_columns(), 1) + bytes(self.data_types)

    def payload(self) -> AnyStr:
        return b''.join([
            value_to_bytes(self.values[i], get_column_size(self.data_types[i]))
            for i in range(self.number_of_columns())
        ])

    def __str__(self):
        return "Record(data_types={}, values={}, size={}, header_size={}, body_size={}, " \
               "number_of_columns={}, bytes={}, header_bytes={}, payload={})" \
            .format(self.data_types, self.values, self.size(), self.header_size(), self.body_size(),
                    self.number_of_columns(), self.as_bytes(), self.header_bytes(), self.payload())


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

    def size(self) -> int:
        return leaf_cell_header_size() + self.payload_size()

    def payload_size(self) -> int:
        return self.record.size()

    def as_bytes(self) -> AnyStr:
        return self.header_bytes() + self.payload()

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(self.record.size(), 2) + int_to_bytes(self.row_id)

    def payload(self) -> AnyStr:
        return self.record.as_bytes()

    def __str__(self):
        return "LeafCell:\nrow_id={}, size={}\nrecord={}\nbytes={}, header_bytes={}, payload={}" \
            .format(self.row_id, self.size(), self.record, self.as_bytes(), self.header_bytes(), self.payload())


class TablePage(DavisBaseSerializable):
    def __init__(self, page_number: int, page_parent: int, cells: Dict = None):
        self.page_number: int = page_number
        self.page_parent: int = page_parent
        if cells is None:
            cells = {}
        self.cells: Dict = cells

    # abstract function
    def insert_record(self, row_id: int, record: Record):
        pass

    # abstract function
    def is_full(self):
        pass

    def remove_record(self, row_id):
        pass

    def __str__(self):
        return 'TablePage(page_number={}, page_parent={})'.format(self.page_number, self.page_parent)


class TableLeafPage(TablePage):
    PAGE_TYPE = 13

    def insert_record(self, row_id: int, record: Record):
        self.cells[row_id] = LeafCell(row_id, record)
        print(self.cells.items())
        print(row_id, LeafCell(row_id, record))

    def add_cell(self, row_id: int, cell: LeafCell = None):
        self.cells[row_id] = cell

    def is_full(self, leaf_cell: LeafCell = None):
        size = self.header_size() + self.payload_size()
        return leaf_cell and size + leaf_cell.size() >= 512 or size >= 512

    def header_size(self) -> int:
        return 13 + 2 * len(self.cells)

    def payload_size(self) -> int:
        return sum([self.cells[row_id].size() for row_id in self.cells])

    def as_bytes(self) -> AnyStr:
        log_debug("as bytes conversion, size", len(self.header_bytes()),
                  len(bytes([0 for i in range(512 - self.header_size() - self.payload_size())])), len(self.payload()))
        log_debug("page payload", self.payload())
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
        locations_bytes = b''
        location = 512
        for row_id in self.cells:
            location -= self.cells[row_id].size()
            locations_bytes += int_to_bytes(location, 2)
        return locations_bytes

    def payload(self) -> AnyStr:
        return b''.join([self.cells[row_id].as_bytes() for row_id in self.cells][::-1])

    def __str__(self):
        parent = super(TableLeafPage, self).__str__()
        cells = "\n".join([str(self.cells[row_id]) for row_id in self.cells])
        return "base={}\nTableLeafPage(header_size={}, payload_size={})\nheader_bytes={}\npayload={}\ncells\n{}" \
            .format(parent, self.header_size(), self.payload_size(), self.header_bytes(), self.payload(), cells)


# Abstract class
class AbstractTableModel:
    def as_list(self) -> List[TablePage]:
        pass

    def add_pages(self, page: List[TablePage]):
        pass

    def insert(self, row_id: int, args: InsertArgs):
        pass

    def update(self, args: UpdateArgs):
        pass

    def delete(self, args):
        pass

    def select(self, args) -> Dict[int, PageCell]:
        pass


def resize_text_data_types(data_types: List[int], record: List[int or str]):
    return [data_types[i] if data_types[i] < 12 else len(record[i]) + 12 for i in range(len(data_types))]


class ListTableModel(AbstractTableModel):
    def __init__(self, columns_metadata: TableColumnsMetadata, pages=None):
        print("model pages", pages)
        if pages is None or len(pages) == 0:
            print("init pages")
            pages = [TableLeafPage(page_number=0, page_parent=0)]
        self.pages: List[TablePage] = pages
        self.current_page: int = 0 if len(self.pages) == 0 else len(self.pages) - 1
        self.columns_metadata: TableColumnsMetadata = columns_metadata

    def as_list(self) -> List[TablePage]:
        return self.pages

    def insert(self, row_id: int, args: InsertArgs):
        if self.pages[self.current_page].is_full():  # is full or is above full with the new element
            self.pages.append(TableLeafPage(self.current_page + 1, 0))
            self.current_page += 1
        self.pages[self.current_page].insert_record(row_id, Record(resize_text_data_types(self.columns_metadata.data_types(), args.record), args.record))
        print()

    def update(self, args: UpdateArgs):
        pass

    def select(self, args: SelectArgs) -> Dict[int, PageCell]:
        records = {}
        for page in self.pages:
            records.update(page.cells)
        return records

    def delete(self, args: DeleteArgs):
        pass

    def __str__(self) -> str:
        return str(self.pages)


class DavisTable(DavisBaseSerializable):
    def __init__(self, name: str, columns_metadata: TableColumnsMetadata = None, pages=None):
        self.name: str = name
        self.model: AbstractTableModel = ListTableModel(columns_metadata, pages)
        self.current_row_id: int = 0
        self.columns_metadata: TableColumnsMetadata = columns_metadata

    def insert(self, args: InsertArgs):
        self.model.insert(self.current_row_id, args)
        self.current_row_id += 1

    def add_pages(self, pages: List[TablePage]):
        self.model.add_pages(pages)

    def update(self, args: UpdateArgs):
        self.model.update(args)

    def select(self, args: SelectArgs) -> Dict[int, PageCell]:
        return self.model.select(args)

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

    def read_byte(self) -> int:
        return self.read_int(1)

    def read_short(self) -> int:
        return self.read_int(2)

    def read_page(self) -> TablePage:
        log_debug("reading page")
        page_type = self.read_byte()
        log_debug("type", page_type)
        number_of_cells = self.read_short()
        log_debug("number_of_cells={}".format(number_of_cells))
        content_area_offset = self.read_short()
        log_debug("content_area_offset={}".format(content_area_offset))
        page_number = self.read_int()
        log_debug("page_number={}".format(page_number))
        page_parent = self.read_int()
        log_debug("page_parent={}".format(page_parent))
        cells_offsets = [self.read_short() for i in range(number_of_cells)]
        log_debug("cells_offsets={}".format(cells_offsets))
        page = TableLeafPage(page_number=page_number, page_parent=page_parent)
        for cell_offset in cells_offsets:
            self.seek(cell_offset)
            log_debug("reading cell at cell_offset={}".format(cell_offset))
            if page_type == TABLE_BTREE_LEAF_PAGE:
                cell_payload_size = self.read_short()
                log_debug("cell_payload_size={}".format(cell_payload_size))
                row_id = self.read_int()
                log_debug("row_id={}".format(row_id))
                number_of_columns = self.read_byte()
                log_debug("number_of_columns", number_of_columns)
                column_data_types = [self.read_byte() for i in range(number_of_columns)]
                log_debug("column_data_types={}".format(column_data_types))
                page.data_types = column_data_types
                values = [bytes_to_value(self.read(get_column_size(column_type)), column_type) for column_type in
                          column_data_types]
                log_debug("values={}".format(values))
                page.add_cell(row_id, LeafCell(row_id, Record(column_data_types, values)))
            if page_type == TABLE_BTREE_INTERIOR_PAGE:
                left_child_page_number = self.read_int()
                row_id = self.read_int()
                # table.add_cell(row_id, LeafCell(row_id, Record(data_types, values)))
        print(" read page ", page.as_bytes())
        return page


class TableFile:

    def __init__(self, path: str):
        self.path = path
        self.table_file = None
        self.file_size = os.path.getsize(self.path)

    def read_pages(self) -> List[TablePage]:
        self.table_file = open(self.path, "rb")
        log_debug("reading", self.file_size, "bytes")
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

    def read_catalog_table(self, name) -> List[TablePage]:
        path = self.catalog_folder_path() + '/' + name + ".tbl"
        if os.path.isfile(path):
            log_debug("catalog table found, reading ", name)
            pages = TableFile(os.path.abspath(path)).read_pages()
            log_debug("pages read", pages)
            return pages
        else:
            log_debug("catalog table not found", name)
        return []

    def read_tables_table(self) -> List[TablePage]:
        return self.read_catalog_table('davisbase_table')

    def write_tables_table(self, table: DavisTable):
        return self.write_catalog_table(table)

    def read_columns_table(self) -> List[TablePage]:
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
            log_debug("final payload", table.as_bytes())
            table_file.write(table.as_bytes())
            table_file.close()

    def write_index(self, index: DavisIndex):
        pass


class DavisBase:
    TABLES_TABLE_COLUMN_METADATA = {
        "rowid": "INT",
        "table_name": "TEXT"
    }

    COLUMNS_TABLE_COLUMN_METADATA = {
        "rowid": "INT",
        "table_name": "TEXT",
        "column_name": "TEXT",
        "data_type": "TEXT",
        "ordinal_position": "TINYINT",
        "is_nullable": "TEXT"
    }

    def __init__(self):
        self.tables: Dict[str, DavisTable] = {}
        self.indexes = {}
        self.fs = DavisBaseFS('../data')

        log_debug("reading catalog tables table")
        table_pages = self.fs.read_tables_table()
        self.tables_table = DavisTable('davisbase_table', TableColumnsMetadata(self.TABLES_TABLE_COLUMN_METADATA), table_pages)
        self.tables_table.current_row_id = 2
        # print("TABLE BYTES", self.tables_table.as_bytes())
        # print("TABLE CONTENT", self.tables_table.model.as_list())
        # print("TABLE pages", len(self.tables_table.model.as_list()))
        # print("TABLE pages", self.tables_table.model.as_list()[0].payload_size())
        if not table_pages:
            self.tables_table.insert(InsertArgs(record=[0, 'davisbase_tables']))
            self.tables_table.insert(InsertArgs(record=[1, 'davisbase_columns']))
            self.tables_table.current_row_id = 2

        # log_debug("reading catalog columns table")
        # columns_pages = self.fs.read_columns_table()
        # self.columns_table = DavisTable('davisbase_columns', TableColumnsMetadata(self.TABLES_TABLE_COLUMN_METADATA))
        # self.columns_table.add_pages(columns_pages)

        result = self.tables_table.select(SelectArgs(['rowid', 'table_name']))
        print(result[0].record)
        # if not columns_pages:
        #     self.tables_table.insert(InsertArgs(record=[3, 3]))

    def show_tables(self) -> List[str]:
        return [table_name for table_name in self.tables] + ['davisbase_tables', 'davisbase_columns']

    def create_table(self, name: str, columns_metadata: TableColumnsMetadata) -> DavisTable:
        table = DavisTable(name, columns_metadata)
        self.fs.write_data_table(table)
        self.tables[name] = table
        self.tables_table.insert(InsertArgs(record=[2, 'name']))
        return table

    def drop_table(self, table_name: str):
        del self.tables[table_name]
        self.tables_table.delete(DeleteArgs()) # delete from metadata
        # delete from file
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
        return self.tables[table_name].select(args)

    def delete(self, table_name: str, args: DeleteArgs):
        self.tables[table_name].delete(args)

    def commit(self):
        log_debug("writing catalog tables table")
        self.fs.write_tables_table(self.tables_table)
        # log_debug("writing catalog columns table")
        # self.fs.write_tables_table(self.columns_table)
        for table_name in self.tables:
            self.fs.write_data_table(self.tables[table_name])
        for index_name in self.indexes:
            self.fs.write_index(self.indexes[index_name])
        # [open(index.name + '.ndx', 'wb').write(index.to_binary()) for index in self.indexes]


davis_base = DavisBase()
davis_base.commit()

print(davis_base.show_tables())

# print(LeafCell(0, Record([1, 2], [1, 2])))
# print(TableLeafPage(0, 0, [1, 2], {0: LeafCell(0, Record([1, 2], [1, 2]))}))

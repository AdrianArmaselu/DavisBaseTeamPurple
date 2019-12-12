from typing import AnyStr, List, Dict
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


def is_int(data_type: int) -> bool:
    return 0 < data_types[data_type] < 5


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


class TableColumnsMetadata:
    def __init__(self, columns=None):
        if columns is None:
            columns = {}
        self.columns: Dict = columns

    def data_types(self) -> List[int]:
        log_debug("CONVERTING TO DATA TYPES", data_type_encodings, self.columns)
        return [data_type_encodings[column_type_index] for column_name, column_type_index in self.columns.items()]


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

    def get_value(self, column_index: int) -> str or int:
        return self.values[column_index]

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


class UpdateArgs:
    def __init__(self, column_index: int, value, condition):
        self.column_index: int = column_index
        self.value = value
        self.condition = condition


class LeafCell(PageCell):
    def __init__(self, row_id: int, record: Record = None):
        super(LeafCell, self).__init__(row_id)
        self.record: Record = record

    def values(self) -> List[str or int]:
        return self.record.values

    def get_value_from_index(self, column_index: int) -> str or int:
        return self.record.get_value(column_index)

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


class Condition:
    def __init__(self, column_index: int, operator: str, value: str):
        self.column_index: int = column_index
        self.operator: str = operator
        self.value: str or int = value

    def is_satisfied(self, cell: LeafCell):
        if isinstance(self.value, str):
            log_debug("casting to int")
            self.value = int(self.value)
        result = {
            "=": lambda a, b: a == b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b
        }[self.operator](cell.get_value_from_index(self.column_index), self.value)
        log_debug("IS SATISFIED", result, self.operator, self.value, cell.get_value_from_index(self.column_index), cell)
        log_debug("IS SATISFIED", type(self.value), type(cell.get_value_from_index(self.column_index)), cell)
        return result


class CreateArgs:
    def __init__(self, columns_metadata: TableColumnsMetadata):
        self.columns_metadata: TableColumnsMetadata = columns_metadata


class DeleteArgs:
    def __init__(self, condition: Condition):
        self.condition: Condition = condition


class SelectArgs:
    def __init__(self, column_names: List[str], condition: Condition = None):
        self.column_names: List[str] = column_names
        self.condition: Condition = condition


class TablePage(DavisBaseSerializable):
    def __init__(self, page_number: int, page_parent: int, cells: Dict[int, PageCell]):
        self.page_number: int = page_number
        self.page_parent: int = page_parent
        self.cells: Dict[int, PageCell] = cells

    def add_cell(self, row_id: int, cell: PageCell):
        self.cells[row_id] = cell

    # abstract function
    def add_record(self, row_id: int, record: Record):
        pass

    # abstract function
    def delete(self, args: DeleteArgs):
        pass

    # abstract function
    def update(self, args: UpdateArgs):
        pass

    def remove_record(self, row_id: int):
        del self.cells[row_id]

    # abstract function
    def values(self) -> List[str or int]:
        pass

    # abstract function
    def add_values(self, row_id: int, data_types: List[int], values: List[str or int]):
        pass

    # abstract function
    def row_count(self) -> int:
        return len(self.cells)

    # abstract function
    def is_full(self):
        pass

    def remove_record(self, row_id):
        pass

    def __str__(self):
        return 'TablePage(page_number={}, page_parent={})'.format(self.page_number, self.page_parent)


class TableLeafPage(TablePage):
    PAGE_TYPE = 13

    def __init__(self, page_number: int, page_parent: int, cells=None):
        super(TableLeafPage, self).__init__(page_number=page_number, page_parent=page_parent, cells=cells)
        if cells is None:
            cells = {}
        self.cells: Dict[int, LeafCell] = cells

    def delete(self, args: DeleteArgs):
        log_debug("BEFORE DELETE", self.cells)
        row_ids_to_be_deleted = []
        for row_id in self.cells:
            if args.condition.is_satisfied(self.cells[row_id]):
                row_ids_to_be_deleted.append(row_id)
        log_debug("deleting rows", row_ids_to_be_deleted, self.cells)
        for row_id in row_ids_to_be_deleted:
            del self.cells[row_id]

    def update(self, args: UpdateArgs):
        for row_id in self.cells:
            if args.condition.is_satisfied(self.cells[row_id]):
                self.cells[row_id].record.values[args.column_index] = args.value

    def values(self) -> List[str or int]:
        return [self.cells[row_id].values() for row_id in self.cells]

    def add_record(self, row_id: int, record: Record):
        self.cells[row_id] = LeafCell(row_id, record)

    def add_values(self, row_id: int, data_types: List[int], values: List[str or int]):
        log_debug("ADD VALUE ", row_id, data_types, values)
        self.cells[row_id] = LeafCell(row_id, Record(data_types, values))

    def get_column_values(self, column_index: int) -> List[str or int]:
        return [self.cells[row_id].get_value_from_index(column_index) for row_id in self.cells]

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


def resize_text_data_types(data_types: List[int], record: List[int or str]):
    return [data_types[i] if data_types[i] < 12 else len(record[i]) + 12 for i in range(len(data_types))]


class DavisTable(DavisBaseSerializable):
    def __init__(self, name: str, columns_metadata: TableColumnsMetadata = None, pages=None):
        self.name: str = name
        self.columns_metadata: TableColumnsMetadata = columns_metadata
        if not pages:
            pages = [TableLeafPage(0, 0)]
        self.pages: List[TablePage] = pages
        self.current_row_id: int = len(self.pages) - 1

    def values(self):
        print("TableLeafPage DavisTable", [page.values() for page in self.pages])
        return [page.values() for page in self.pages]

    def row_count(self):
        return sum([page.row_count() for page in self.pages])

    def select(self, args: SelectArgs) -> List[str or int]:
        return flatten([page.values() for page in self.pages])

    def current_page(self) -> TablePage:
        return self.pages[len(self.pages) - 1]

    def insert(self, rows: List[List[str or int]], column_names: List[str] = None):
        if self.current_page().is_full():
            self.pages.append(TableLeafPage(len(self.pages), 0))

        for row in rows:
            self.current_page().add_values(self.current_row_id,
                                           resize_text_data_types(self.columns_metadata.data_types(), row),
                                           row)
            self.current_row_id += 1

    def update(self, args: UpdateArgs):
        for page in self.pages:
            page.update(args)

    def delete(self, args: DeleteArgs):
        for page in self.pages:
            page.delete(args)

    def as_bytes(self) -> bytes:
        content = b''
        for page in self.pages:
            content += page.as_bytes()
        return content

    def __str__(self) -> str:
        return str(self.pages)


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

    def read_storage_table(self, name):
        path = self.storage_folder_path() + '/' + name + ".tbl"
        if os.path.isfile(path):
            log_debug("storage table found, reading ", name)
            pages = TableFile(os.path.abspath(path)).read_pages()
            log_debug("pages read", pages)
            return pages
        else:
            log_debug("storage table not found", name)
        return []

    def read_tables_table(self) -> List[TablePage]:
        return self.read_catalog_table('davisbase_table')

    def write_tables_table(self, table: DavisTable):
        return self.write_catalog_table(table)

    def read_columns_table(self) -> List[TablePage]:
        return self.read_catalog_table('davisbase_columns')

    def write_columns_table(self, table: DavisTable):
        return self.write_catalog_table(table)

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

    TEST = {
        "v1": "INT"
    }

    def __init__(self):
        self.tables: Dict[str, DavisTable] = {}
        self.indexes = {}
        self.fs = DavisBaseFS('../data')

        log_debug("reading catalog tables table")
        table_pages = self.fs.read_tables_table()
        tables_metadata = TableColumnsMetadata(self.TABLES_TABLE_COLUMN_METADATA)
        self.davisbase_tables = DavisTable('davisbase_table', tables_metadata, table_pages)
        if self.davisbase_tables.row_count() == 0:
            self.davisbase_tables.insert([[1, 'davisbase_tables'], [2, 'davisbase_columns']])
        self.davisbase_tables.current_row_id = self.davisbase_tables.row_count() + 1
        result = self.davisbase_tables.select(SelectArgs(['rowid', 'table_name']))
        log_debug('TABLES', result)

        log_debug("reading catalog columns table")
        columns_pages = self.fs.read_columns_table()
        columns_metadata = TableColumnsMetadata(self.TABLES_TABLE_COLUMN_METADATA)
        self.davisbase_columns = DavisTable('davisbase_columns', columns_metadata, columns_pages)
        if self.davisbase_columns.row_count() == 0:
            self.davisbase_columns.insert([
                [1, 'davis_tables', 'rowid', 'INT', 1, 'NO'],
                [2, 'davis_tables', 'table_name', 'TEXT', 2, 'NO'],
                [3, 'davisbase_columns', 'rowid', 'INT', 1, 'NO'],
                [4, 'davisbase_columns', 'table_name', 'TEXT', 2, 'NO'],
                [5, 'davisbase_columns', 'column_name', 'TEXT', 3, 'NO'],
                [6, 'davisbase_columns', 'data_type', 'TEXT', 4, 'NO'],
                [7, 'davisbase_columns', 'ordinal_position', 'TINYINT', 5, 'NO'],
                [8, 'davisbase_columns', 'is_nullable', 'TEXT', 6, 'NO']])
        result = self.davisbase_columns.select(SelectArgs(['rowid', 'table_name']))
        log_debug('COLUMNS', result)

        self.create_table('t1', TableColumnsMetadata(self.TEST))
        self.tables['t1'].insert([[1]])
        log_debug("CREATED TABLES", self.tables)
        result = self.tables['t1'].select(SelectArgs(['rowid', 'table_name']))
        log_debug('TABLES t1', result)

    def show_tables(self) -> List[str]:
        rows = self.davisbase_tables.select(SelectArgs(['rowid', 'table_name']))
        return [row[1] for row in rows]

    def create_table(self, name: str, columns_metadata: TableColumnsMetadata) -> DavisTable:
        table = DavisTable(name, columns_metadata)
        self.tables[name] = table
        self.davisbase_tables.insert([[self.davisbase_tables.current_row_id, name]])
        return table

    def drop_table(self, table_name: str):
        if table_name in self.tables:
            del self.tables[table_name]
        self.davisbase_tables.delete(DeleteArgs(Condition(column_index=1, operator='=', value=table_name)))
        # delete in columns
        # delete from file if is on file
        pass

    def create_index(self):
        # Index_Btree(self,5)
        pass

    def select(self, table_name: str, args: SelectArgs) -> List[str or int]:
        self.load_table_if_not_loaded(table_name)
        return self.tables[table_name].select(args)

    def insert(self, table_name: str, rows: List[str or int], column_names: List[str] = None):
        self.load_table_if_not_loaded(table_name)
        self.tables[table_name].insert([rows], column_names)

    def update(self, table_name: str, args: UpdateArgs):
        self.load_table_if_not_loaded(table_name)
        self.tables[table_name].update(args)

    def delete(self, table_name: str, args: DeleteArgs):
        self.load_table_if_not_loaded(table_name)
        self.tables[table_name].delete(args)

    def load_table_if_not_loaded(self, table_name: str):
        if table_name not in self.tables:
            pages = self.fs.read_catalog_table(table_name)
            table = DavisTable(table_name, TableColumnsMetadata(self.TEST), pages)
            self.tables[table_name] = table
        return None

    def commit(self):
        log_debug("writing catalog tables table")
        self.fs.write_tables_table(self.davisbase_tables)
        log_debug("writing catalog columns table")
        self.fs.write_tables_table(self.davisbase_columns)
        for table_name in self.tables:
            self.fs.write_data_table(self.tables[table_name])
        for index_name in self.indexes:
            self.fs.write_index(self.indexes[index_name])
        # [open(index.name + '.ndx', 'wb').write(index.to_binary()) for index in self.indexes]

# davis_base = DavisBase()
# davis_base.commit()

# print(davis_base.show_tables())

# print(LeafCell(0, Record([1, 2], [1, 2])))
# print(TableLeafPage(0, 0, [1, 2], {0: LeafCell(0, Record([1, 2], [1, 2]))}))

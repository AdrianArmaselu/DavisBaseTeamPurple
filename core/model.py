import os
import math
from typing import AnyStr, List, Dict
from io import BytesIO

from core.datum import DavisBaseType, Null

# Constants
from core.util import int_to_bytes, data_type_encodings, bytes_to_int, log_debug, flatten, leaf_cell_header_size, \
    get_column_size, DATA_TYPES

INDEX_BTREE_INTERIOR_PAGE = 2
INDEX_BTREE_LEAF_PAGE = 10

# Constants
TABLE_BTREE_INTERIOR_PAGE = 5
TABLE_BTREE_LEAF_PAGE = 13


class ColumnDefinition:
    def __init__(self, data_type_str: str, index: int):
        self.data_type_int: int = data_type_encodings[data_type_str]
        self.data_type_str: str = data_type_str
        self.index: int = index
        self.data_type: DavisBaseType = DATA_TYPES[self.data_type_int]


class TableColumnsMetadata:
    def __init__(self, columns: Dict[str, ColumnDefinition] = None):
        if columns is None:
            columns = {}
        self.columns: Dict[str, ColumnDefinition] = columns

    def value(self, name: str, value: str) -> DavisBaseType:
        return DATA_TYPES[self.columns[name].data_type_int](value)

    def index(self, name: str) -> int:
        return self.columns[name].index

    def column_definition(self, name: str) -> ColumnDefinition:
        return self.columns[name]

    def data_type_ints(self) -> List[int]:
        return [definition.data_type_int for column_name, definition in self.columns.items()]


class Record:
    def __init__(self, values: List[DavisBaseType]):
        self.values: List[DavisBaseType] = values

    def set(self, index: int, value: DavisBaseType):
        self.values[index] = value

    def header_size(self) -> int:
        return 1 + len(self.values)

    def body_size(self) -> int:
        return sum([len(value) for value in self.values])

    def header_bytes(self) -> bytes:
        return int_to_bytes(len(self.values), 1) + bytes([value.get_type_number() for value in self.values])

    def payload(self) -> bytes:
        return b''.join([bytes(value) for value in self.values])

    def __len__(self) -> int:
        return self.header_size() + self.body_size()

    def __getitem__(self, index: int) -> DavisBaseType:
        return self.values[index]

    def __bytes__(self) -> bytes:
        return self.header_bytes() + self.payload()

    def __str__(self) -> str:
        return str([str(value) for value in self.values])


class PageCell:
    def __init__(self, row_id: int):
        self.row_id = row_id


class InternalCell(PageCell):
    def __init__(self, row_id: int, left_child_page: int):
        super(InternalCell, self).__init__(row_id)
        self.left_child_page: int = left_child_page

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(self.left_child_page) + int_to_bytes(self.row_id)

    def __bytes__(self) -> AnyStr:
        return self.header_bytes()


class LeafCell(PageCell):
    def __init__(self, row_id: int, record: Record = None):
        super(LeafCell, self).__init__(row_id)
        self.record: Record = record

    def set(self, index: int, value: DavisBaseType):
        self.record.set(index, value)

    def values(self) -> List[DavisBaseType]:
        return self.record.values

    def header_bytes(self) -> AnyStr:
        return int_to_bytes(len(self.record), 2) + int_to_bytes(self.row_id)

    def payload(self) -> AnyStr:
        return bytes(self.record)

    def __len__(self) -> int:
        return leaf_cell_header_size() + len(self.record)

    def __getitem__(self, index: int) -> DavisBaseType:
        return self.record[index]

    def __bytes__(self) -> AnyStr:
        return self.header_bytes() + self.payload()

    def __str__(self) -> str:
        return "{}: {}".format(self.row_id, self.record)


class Condition:
    def __init__(self, column_index: int, operator: str, value: DavisBaseType):
        self.column_index: int = column_index
        self.operator: str = operator
        self.value: DavisBaseType = value

    def is_satisfied(self, cell: LeafCell):
        result = {
            "=": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b
        }[self.operator](cell[self.column_index], self.value)
        return result


class CreateArgs:
    def __init__(self, columns_metadata: TableColumnsMetadata):
        self.columns_metadata: TableColumnsMetadata = columns_metadata


class DeleteArgs:
    def __init__(self, condition: Condition):
        self.condition: Condition = condition


class SelectArgs:
    def __init__(self, column_indexes: List[int], condition: Condition = None):
        self.column_indexes: List[int] = column_indexes
        self.condition: Condition = condition


class UpdateArgs:
    def __init__(self, column_index: int, value: DavisBaseType, condition: Condition):
        self.column_index: int = column_index
        self.value = value
        self.condition = condition


class TablePage:
    def __init__(self, page_number: int, page_parent: int, cells: Dict[int, PageCell]):
        self.page_number: int = page_number
        self.page_parent: int = page_parent
        self.cells: Dict[int, PageCell] = cells

    def select(self, args: SelectArgs):
        pass

    def insert(self, row_id: int, cell: LeafCell):
        self.cells[row_id] = cell

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
    def row_count(self) -> int:
        return len(self.cells)

    # abstract function
    def is_full(self, leaf_cell):
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

    def select(self, args: SelectArgs):
        selected = []
        for row_id in self.cells:
            if args.condition.is_satisfied(self.cells[row_id]):
                selected.append([self.cells[row_id].values()[i] for i in args.column_indexes])
        return selected

    def update(self, args: UpdateArgs):
        for row_id in self.cells:
            if args.condition.is_satisfied(self.cells[row_id]):
                self.cells[row_id].record.values[args.column_index] = args.value

    def delete(self, args: DeleteArgs):
        row_ids_to_be_deleted = []
        for row_id in self.cells:
            if args.condition.is_satisfied(self.cells[row_id]):
                row_ids_to_be_deleted.append(row_id)
        for row_id in row_ids_to_be_deleted:
            del self.cells[row_id]

    def values(self) -> List[str or int]:
        return [self.cells[row_id].values() for row_id in self.cells]

    def add_record(self, row_id: int, record: Record):
        self.cells[row_id] = LeafCell(row_id, record)

    def get_column_values(self, column_index: int) -> List[str or int]:
        return [self.cells[row_id][column_index] for row_id in self.cells]

    def add_cell(self, row_id: int, cell: LeafCell = None):
        self.cells[row_id] = cell

    def is_full(self, leaf_cell: LeafCell = None):
        size = self.header_size() + self.payload_size()
        return leaf_cell and size + len(leaf_cell) >= 512 or size >= 512

    def header_size(self) -> int:
        return 13 + 2 * len(self.cells)

    def payload_size(self) -> int:
        return sum([len(self.cells[row_id]) for row_id in self.cells])

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
            location -= len(self.cells[row_id])
            locations_bytes += int_to_bytes(location, 2)
        return locations_bytes

    def payload(self) -> AnyStr:
        return b''.join([bytes(self.cells[row_id]) for row_id in self.cells][::-1])

    def __bytes__(self) -> AnyStr:
        return self.header_bytes() \
               + bytes([0 for _ in range(512 - self.header_size() - self.payload_size())]) \
               + self.payload()

    def __len__(self):
        return len(self.header_bytes()) + len(self.payload())

    def __str__(self):
        return "{" + ", ".join([str(self.cells[row_id]) for row_id in self.cells]) + "}"


def resize_text_data_types(data_types: List[int], record: List[int or str]):
    return [data_types[i] if data_types[i] < 12 else len(record[i]) + 12 for i in range(len(data_types))]


class DavisTable:
    def __init__(self, name: str, current_row_id: int = 1, columns_metadata: TableColumnsMetadata = None, pages=None):
        self.name: str = name
        self.columns_metadata: TableColumnsMetadata = columns_metadata
        if not pages:
            pages = [TableLeafPage(0, 0)]
        self.pages: List[TablePage] = pages
        self.current_row_id: int = current_row_id

    def select(self, column_name: str, operator: str, value: str, column_names: List[str] = None) -> List[DavisBaseType]:
        index = self.columns_metadata.index(column_name)
        value = self.columns_metadata.value(column_name, value)

        if not column_names or column_names[0] == "*":
            args = SelectArgs([i for i in range(len(self.columns_metadata.columns))], Condition(index, operator, value))
        else:
            args = SelectArgs([self.columns_metadata.index(n) for n in column_names], Condition(index, operator, value))
        return flatten([page.select(args) for page in self.pages])

    def insert(self, records: List[List[str]], column_names: List[str] = None):

        for record in records:
            values = [Null() for _ in record]
            if column_names:
                index = 0
                for column_name in column_names:
                    column_definition = self.columns_metadata.column_definition(column_name)
                    values[column_definition.index] = DATA_TYPES[column_definition.data_type_int](record[index])
                    index += 1
            else:
                values = []
                for index in range(len(record)):
                    data_types = self.columns_metadata.data_type_ints()
                    values.append(DATA_TYPES[data_types[index]](record[index]))

            cell = LeafCell(self.current_row_id, Record(values))
            if self.current_page().is_full(cell):
                self.pages.append(TableLeafPage(len(self.pages), 0))
            self.current_row_id += 1
            self.current_page().add_cell(self.current_row_id, cell)

    def update(self, column_name: str, value: str, condition_column_name: str, operator: str,
               condition_column_value: str):
        index = self.columns_metadata.index(column_name)
        update_value = self.columns_metadata.value(column_name, value)
        condition_index = self.columns_metadata.index(condition_column_name)
        condition_value = self.columns_metadata.value(condition_column_name, condition_column_value)
        for page in self.pages:
            page.update(UpdateArgs(index, update_value, Condition(condition_index, operator, condition_value)))

    def delete(self, condition_column_name: str, operator: str, condition_column_value: str):
        index = self.columns_metadata.index(condition_column_name)
        value = self.columns_metadata.value(condition_column_name, condition_column_value)
        for page in self.pages:
            page.delete(DeleteArgs(Condition(index, operator, value)))

    def values(self):
        return [page.values() for page in self.pages]

    def row_count(self):
        return sum([page.row_count() for page in self.pages])

    def current_page(self) -> TablePage:
        return self.pages[len(self.pages) - 1]

    def __bytes__(self) -> bytes:
        return b''.join([bytes(page) for page in self.pages])

    def __str__(self) -> str:
        return str([str(page) for page in self.pages])


class DavisIndex:
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
                values = [DATA_TYPES[column_type](self.read(get_column_size(column_type))) for column_type in
                          column_data_types]
                log_debug("values={}".format([str(v) for v in values]))
                page.add_cell(row_id, LeafCell(row_id, Record(values)))
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
        pages = [self.read_page() for _ in range(math.ceil(self.file_size / 512))]
        self.close()
        return pages

    def write(self, table: DavisTable):
        self.table_file = open(self.path, "wb")
        self.table_file.write(bytes(table))
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
            pages = TableFile(os.path.abspath(path)).read_pages()
            # log_debug("pages read", pages)
            return pages
        # else:
        #     log_debug("catalog table not found", name)
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
            table_file.write(bytes(table))
            table_file.close()

    def write_index(self, index: DavisIndex):
        pass


class DavisBase:
    TABLES_TABLE_COLUMN_METADATA = {
        "rowid": ColumnDefinition("INT", 0),
        "table_name": ColumnDefinition("TEXT", 1),
        "table_rowid": ColumnDefinition("INT", 2)
    }

    COLUMNS_TABLE_COLUMN_METADATA = {
        "rowid": ColumnDefinition("INT", 0),
        "table_name": ColumnDefinition("TEXT", 1),
        "column_name": ColumnDefinition("TEXT", 2),
        "data_type": ColumnDefinition("TEXT", 3),
        "ordinal_position": ColumnDefinition("TINYINT", 4),
        "is_nullable": ColumnDefinition("TEXT", 5)
    }

    def __init__(self):
        self.tables: Dict[str, DavisTable] = {}
        self.indexes = {}
        self.fs = DavisBaseFS(os.path.dirname(__file__) + '/../data')

        table_pages = self.fs.read_tables_table()
        tables_metadata = TableColumnsMetadata(self.TABLES_TABLE_COLUMN_METADATA)
        self.davisbase_tables = DavisTable('davisbase_table', columns_metadata=tables_metadata, pages=table_pages)
        self.davisbase_tables.current_row_id = self.davisbase_tables.row_count() + 1
        if self.davisbase_tables.row_count() == 0:
            self.davisbase_tables.insert([[1, 'davisbase_tables', 2], [2, 'davisbase_columns', 9]])
        columns_pages = self.fs.read_columns_table()
        columns_metadata = TableColumnsMetadata(self.COLUMNS_TABLE_COLUMN_METADATA)
        self.davisbase_columns = DavisTable('davisbase_columns', columns_metadata=columns_metadata, pages=columns_pages)
        self.davisbase_columns.current_row_id = self.davisbase_columns.row_count() + 1
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
        self.tables['davisbase_tables'] = self.davisbase_tables
        self.tables['davisbase_columns'] = self.davisbase_tables

    def show_tables(self):
        rows = self.davisbase_tables.select("rowid", ">=", "0", ['table_name'])
        for row in rows:
            for c in row:
                print(str(c))

    def create_table(self, name: str, columns_metadata: TableColumnsMetadata) -> DavisTable:
        table = DavisTable(name, columns_metadata=columns_metadata)
        self.tables[name] = table
        self.davisbase_tables.insert([[self.davisbase_tables.current_row_id, name, 0]])

        columns = []
        current_row_id = self.davisbase_columns.row_count()
        position = 0
        for column_name in columns_metadata.columns:
            columns.append(
                [current_row_id, name, column_name, columns_metadata.column_definition(column_name).data_type_str,
                 position, 'YES'])
            current_row_id += 1
            position += 1
        self.davisbase_columns.insert(columns)

        return table

    def drop_table(self, table_name: str):
        if table_name in self.tables:
            del self.tables[table_name]
        self.davisbase_tables.delete('table_name', "=", table_name)

    def create_index(self):
        # Index_Btree(self,5)
        pass

    def select(self, table_name: str, column_name: str, operator: str, value: str, column_names: List[str] = None) -> List[
        DavisBaseType]:
        self.load_table_if_not_loaded(table_name)
        return self.tables[table_name].select(column_name, operator, value, column_names)

    def insert(self, table_name: str, rows: List[str], column_names: List[str] = None):
        self.load_table_if_not_loaded(table_name)
        self.tables[table_name].insert([rows], column_names)
        self.davisbase_tables.update("table_rowid", str(self.tables[table_name].current_row_id), "table_name", "=",
                                     table_name)

    def update(self, table_name: str, column_name: str, value: str, condition_column_name: str, operator: str,
               condition_column_value: str):
        self.load_table_if_not_loaded(table_name)
        self.tables[table_name].update(column_name, value, condition_column_name, operator, condition_column_value)

    def delete(self, table_name: str, condition_column_name: str, operator: str, condition_column_value: str):
        self.load_table_if_not_loaded(table_name)
        self.tables[table_name].delete(condition_column_name, operator, condition_column_value)

    def load_table_if_not_loaded(self, table_name: str):
        if table_name not in self.tables:
            pages = self.fs.read_storage_table(table_name)
            result = self.davisbase_columns.select( 'table_name', "=",
                                                   table_name,['column_name', 'data_type', 'ordinal_position'])
            metadata = {}
            for r in result:
                name = r[0]
                data_type =r[1]
                position =r[2]
                metadata[name.value] = ColumnDefinition(data_type.value, position.value)
            table = DavisTable(table_name, columns_metadata=TableColumnsMetadata(metadata), pages=pages)
            self.tables[table_name] = table
        return None

    def commit(self):
        for table_name in self.tables:
            if table_name == 'davisbase_tables':
                self.fs.write_catalog_table(self.davisbase_tables)
                continue
            elif table_name == 'davisbase_columns':
                self.fs.write_catalog_table(self.davisbase_columns)
                continue
            self.fs.write_data_table(self.tables[table_name])
        for index_name in self.indexes:
            self.fs.write_index(self.indexes[index_name])

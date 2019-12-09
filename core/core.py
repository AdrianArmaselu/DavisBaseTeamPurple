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


class TablePage(DavisBaseSerializable):
    def __init__(self, page_number: int, page_parent: int, data_types, rows: Dict = None):
        if rows is None:
            rows = {}
        self.rows: Dict = rows
        self.page_number: int = page_number
        self.page_parent: int = page_parent
        self.data_types = data_types

    def row_count(self):
        return len(self.rows)

    def insert_record(self, row_id: int, record: List):
        self.rows[row_id] = record

    # abstract function
    def is_full(self):
        pass

    def remove_record(self, row_id):
        for i in range(len(self.rows)):
            if self.rows[i].row_id == row_id:
                del self.rows[i]

    def __str__(self):
        return 'test'


class TableLeafPage(TablePage):
    PAGE_TYPE = 13

    def insert(self, row_id, row: List):
        self.rows[row_id] = row

    def header_bytes(self) -> AnyStr:
        page_type = int_to_bytes(self.PAGE_TYPE, 1)
        number_of_cells = int_to_bytes(len(self.rows), 2)
        page_number = int_to_bytes(self.page_number)
        page_parent = int_to_bytes(self.page_parent)
        content_area_offset = int_to_bytes(512 - len(self.cells_bytes()), 2)
        return page_type + b'\x00' + number_of_cells + content_area_offset + page_number + page_parent + b'\x00\x00' + self.cells_offsets_bytes()

    def cells_offsets_bytes(self) -> AnyStr:
        offsets = b''
        offset = 512
        for cell_bytes in self.cells_bytes_list():
            offset -= len(cell_bytes)
            offsets += int_to_bytes(offset, 2)
        return offsets

    def cells_bytes_list(self) -> List[AnyStr]:
        return [cell.as_bytes() for cell in self.rows]

    def rows_bytes(self) -> AnyStr:
        values_bytes = b''
        for page_row in self.rows:
            values_bytes += self.row_bytes(page_row)
        return values_bytes

    def row_bytes(self, page_row) -> AnyStr:
        row_bytes = b''
        for i in range(len(self.data_types)):
            column_size = get_column_size(self.data_types[i])
            column_value = page_row[i]
            row_bytes += bytes(column_value, 'utf-8') if isinstance(column_value, str) else int_to_bytes(column_value, column_size)
        return row_bytes

    def cells_bytes(self) -> AnyStr:
        cells_bytes = b''
        for row_id in self.rows:
            page_row = self.rows[row_id]
            cell_payload_size_bytes = int_to_bytes(len(self.data_types) + sum(self.data_types), 2)
            row_id_bytes = int_to_bytes(row_id)
            number_of_columns_bytes = int_to_bytes(len(page_row))
            data_types_bytes = bytes(self.data_types)
            cells_bytes += cell_payload_size_bytes + row_id_bytes + number_of_columns_bytes + data_types_bytes + self.row_bytes(page_row)
        return cells_bytes

    def record_byte_size(self):
        # cell payload size + rowid + number of columns + data_types + values
        return 7 + len(self.data_types) + sum([get_column_size(data_type) for data_type in self.data_types])

    def as_bytes(self) -> AnyStr:
        header_bytes = self.header_bytes()
        cells_bytes = self.cells_bytes()
        empty_bytes = bytes([0 for i in range(512 - len(header_bytes) - len(cells_bytes))])
        return header_bytes + empty_bytes + cells_bytes

    def is_full(self):
        return len(self.header_bytes()) + len(self.cells_bytes()) >= 512

    def __str__(self):
        return str(self.rows)


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
    def __init__(self, name: str, pages: List[TablePage] = None):
        if pages is None:
            pages = [TableLeafPage(0, 0)]
        self.name: str = name
        self.pages: List[TablePage] = pages
        self.current_page: int = 0 if len(pages) == 0 else len(pages) - 1

    def as_list(self) -> List[TablePage]:
        return self.pages

    def insert(self, row_id: int, args: InsertArgs):
        if self.pages[self.current_page].is_full():  # is full or is above full with the new element
            self.pages.append(TableLeafPage(self.current_page + 1, 0))
            self.current_page += 1
        self.pages[self.current_page].insert_record(row_id, args.record)

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
    def __init__(self, current_row_id: int = 0, pages=None, name: str = None):
        self.name: str = name
        self.model: AbstractTableModel = ListTableModel(name, pages)
        self.current_row_id: int = current_row_id

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
        self.skip(1)
        number_of_cells = self.read_short()
        content_area_offset = self.read_short()
        page_number = self.read_int()
        page_parent = self.read_int()
        self.skip(2)
        cells_offsets = self.read(2 * number_of_cells)

        page_rows: Dict = {}
        for cell_offset in cells_offsets:
            self.seek(cell_offset)
            if page_type == TABLE_BTREE_LEAF_PAGE:
                cell_payload_size = self.read_short()
                row_id = self.read_int()
                number_of_columns = self.read_byte()
                column_data_types = self.read(number_of_columns)
                record_columns: List = []
                for column_type in column_data_types:
                    value_bytes = self.read(get_column_size(column_type))
                    column_value = bytes_to_int(value_bytes) if column_type < 12 else str(value_bytes)
                    record_columns.append(column_value)
                page_rows[row_id] = record_columns
            if page_type == TABLE_BTREE_INTERIOR_PAGE:
                left_child_page_number = self.read_int()
                row_id = self.read_int()
                page_rows[row_id] = left_child_page_number
        return TableLeafPage(page_number, page_parent, page_rows)


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
            return DavisTable(name=name, pages=pages)
        return DavisTable(name=name)

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

        if self.tables_table.current_row_id == 0:
            self.tables_table.insert(InsertArgs([3]))

    def show_tables(self) -> List[str]:
        return [table_name for table_name in self.tables]

    def create_table(self):
        # create table object
        # update metadata table
        pass

    def drop_table(self):
        # delete table
        # update metadata
        pass

    def create_index(self):
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
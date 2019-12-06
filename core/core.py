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

# Constants
NULL = 0
TINYINT = 1
SMALLINT = 2
INT = 3
BIGINT = 4
FLOAT = 5
DOUBLE = 6
YEAR = 8
TIME = 9
DATETIME = 10
DATE = 11
TEXT = 12


def flatten(input_list: List or bytes) -> List or bytes:
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
    def as_bytes(self) -> List:
        pass

    def from_bytes(self):
        pass


class RecordColumn:
    def __init__(self, column_type: int, column_value: int or str):
        self.column_type: int = column_type
        self.column_value: int or str = column_value
        self.column_size: int = get_column_size(column_type)

    def value_bytes(self) -> List:
        return int_to_bytes(self.column_value, self.column_size) if self.column_size < 12 \
            else bytes(self.column_value, 'utf-8')

    def type_bytes(self) -> List:
        return int_to_bytes(self.column_type, 1)


class InsertArgs:
    def __init__(self, record: List[RecordColumn]):
        self.record: List[RecordColumn] = record


class UpdateArgs:
    def __init__(self, column_name: str, value, condition):
        self.column_name: str = column_name
        self.value = value
        self.condition = condition


class DeleteArgs:
    def __init__(self):
        pass


class SelectArgs:
    def __init__(self):
        pass


class TableCell(DavisBaseSerializable):
    def __init__(self, row_id):
        self.row_id: int = row_id

    def payload_size(self) -> int:
        return len(self.as_bytes())

    def from_bytes(self) -> 'TableCell':
        pass


class TableLeafCell(TableCell):
    def __init__(self, row_id: int, columns: List[RecordColumn]):
        super(TableLeafCell, self).__init__(row_id)
        self.row_id: int = row_id
        self.columns: List[RecordColumn] = columns

    def number_of_columns(self) -> int:
        return len(self.columns)

    def as_bytes(self) -> List:
        record_body = flatten([column.value_bytes() for column in self.columns])
        record_types = flatten([[column.type_bytes() for column in self.columns]])
        number_of_columns_bytes = int_to_bytes(self.number_of_columns(), 1)
        row_id_bytes = int_to_bytes(self.row_id)
        bytes_of_cell_payload = int_to_bytes(1 + len(record_types) + len(record_body))
        return flatten([bytes_of_cell_payload, row_id_bytes, number_of_columns_bytes, record_types, record_body])


class TableInteriorCell(TableCell):
    def __init__(self, row_id: int, left_child_page_number: int):
        super(TableInteriorCell, self).__init__(row_id)
        self.left_child_page_number: int = left_child_page_number

    def as_bytes(self) -> List:
        row_id_bytes = int_to_bytes(self.row_id)
        page_number_bytes = int_to_bytes(self.left_child_page_number)
        return flatten([page_number_bytes, row_id_bytes])


class TablePage(DavisBaseSerializable):
    def __init__(self, page_number: int, page_parent: int, cells: List[TableCell] = None):
        if cells is None:
            cells = []
        self.cells: List[TableCell] = cells
        self.page_number: int = page_number
        self.page_parent: int = page_parent

    def insert_record(self, row_id: int, record: List[RecordColumn]):
        self.cells.append(TableLeafCell(row_id, record))

    # abstract function
    def is_full(self):
        pass

    def remove_record(self, row_id):
        for i in range(len(self.cells)):
            if self.cells[i].row_id == row_id:
                del self.cells[i]


class TableLeafPage(TablePage):
    PAGE_TYPE = 13

    def insert(self, cell: TableLeafCell):
        self.cells.append(cell)

    def header_bytes(self) -> List:
        page_type = [int_to_bytes(self.PAGE_TYPE, 1)]
        number_of_cells = [int_to_bytes(len(self.cells), 2)]
        content_area_offset = [-1]  # todo
        page_number = [self.page_number]
        page_parent = [self.page_parent]
        return flatten([page_type, [0], number_of_cells, content_area_offset, page_number, page_parent, [0, 0]])

    def cells_bytes(self) -> List:
        return flatten([cell.as_bytes() for cell in self.cells])

    def as_bytes(self) -> List:
        header_bytes = self.header_bytes()
        empty_bytes = [0]  # todo
        cells_bytes = self.cells_bytes()
        return flatten([header_bytes, empty_bytes, cells_bytes])

    def is_full(self):
        return len(self.header_bytes()) + len(self.cells_bytes()) >= 512


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
    def __init__(self, pages: List[TablePage] = None):
        if pages is None:
            pages = [TableLeafPage(0, 0)]
        self.pages: List[TablePage] = pages
        self.current_page: int = len(pages) - 1

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
        pass

    def delete(self, args: DeleteArgs):
        pass


class DavisTable(DavisBaseSerializable):
    def __init__(self, current_row_id: int = 0, pages=None, name: str = None):
        self.name: str = name
        self.model: AbstractTableModel = ListTableModel(pages)
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
        return flatten([page.as_bytes() for page in self.model.as_list()])


class DavisIndex(DavisBaseSerializable):
    def __init__(self, name):
        self.name = name


class Catalog(DavisBaseSerializable):
    def __init__(self):
        self.davisbase_tables = DavisBaseFS().read_tables_table()
        self.davisbase_columns = DavisBaseFS().read_columns_table()
        self.davisbase_tables.insert(RecordColumn(INT, 1), RecordColumn(TEXT, 'davisbase_tables'))
        self.davisbase_tables.insert(RecordColumn(INT, 2), RecordColumn(TEXT, 'davisbase_columns'))

    def load_tables_table(self):
        pass

    def load_columns_table(self):
        pass


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

        cells: List[TableCell] = []
        for cell_offset in cells_offsets:
            self.seek(cell_offset)
            if page_type == TABLE_BTREE_LEAF_PAGE:
                cell_payload_size = self.read_short()
                row_id = self.read_int()
                number_of_columns = self.read_byte()
                column_data_types = self.read(number_of_columns)
                record_columns: List[RecordColumn] = []
                for column_type in column_data_types:
                    value_bytes = self.read(get_column_size(column_type))
                    column_value = bytes_to_int(value_bytes) if column_type < 12 else str(value_bytes)
                    record_columns.append(RecordColumn(column_type, column_value))
                cells.append(TableLeafCell(row_id, record_columns))
            if page_type == TABLE_BTREE_INTERIOR_PAGE:
                left_child_page_number = self.read_int()
                row_id = self.read_int()
                cells.append(TableInteriorCell(row_id, left_child_page_number))
        return TablePage(page_number, page_parent, cells)


class TableFile:

    def __init__(self, path: str):
        self.path = path
        self.table_file = None
        self.file_size = os.path.getsize(self.path)

    def read(self) -> DavisTable:
        self.table_file = open(self.path, "rb")
        pages = [self.read_page() for page in range(math.ceil(self.file_size / 512))]
        self.close()
        return DavisTable(pages=pages)

    def write(self, table: DavisTable):
        self.table_file = open(self.path, "wb")
        self.table_file.write(table.as_bytes())
        self.table_file.close()

    def read_page(self) -> TablePage:
        return PageReader(self.table_file.read(512)).read_page()

    def close(self):
        self.table_file.close()


class DavisBaseFS:
    CATALOG_FOLDER_PATH = 'catalog'
    DATA_FOLDER_PATH = 'storage'

    def __init__(self, folder: str):
        self.folder: str = folder
        if not os.path.exists(self.catalog_folder_path()):
            os.makedirs(self.catalog_folder_path())
        if not os.path.exists(self.storage_folder_path()):
            os.makedirs(self.storage_folder_path())

    def catalog_folder_path(self) -> str:
        return self.folder + '/' + self.CATALOG_FOLDER_PATH

    def storage_folder_path(self) -> str:
        return self.folder + "/" + self.DATA_FOLDER_PATH

    def read_catalog_table(self, name):
        path = self.catalog_folder_path() + '/' + name + ".tbl"
        if os.path.isfile(path):
            table = TableFile(path).read()
            table.name = name
            return table
        return DavisTable(name=name)

    def read_tables_table(self) -> DavisTable:
        return self.read_catalog_table('davisbase_table')

    def read_columns_table(self) -> DavisTable:
        return self.read_catalog_table('davisbase_columns')

    def get_table(self, name: str):
        pass

    def write_table(self, table: DavisTable):
        path = self.storage_folder_path() + '/' + table.name + ".tbl"
        TableFile(path).write(table)


class DavisBase:
    def __init__(self):
        self.tables: Dict[str, DavisTable] = {}
        self.indexes = {}
        self.fs = DavisBaseFS('data')
        self.load_catalog()

    def load_catalog(self):
        tables_table = self.fs.read_tables_table()
        columns_table = self.fs.read_columns_table()
        self.tables[tables_table.name] = tables_table
        self.tables[columns_table.name] = columns_table

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
        for table_name in self.tables:
            self.fs.write_table(self.tables[table_name])
        # [open(index.name + '.ndx', 'wb').write(index.to_binary()) for index in self.indexes]


davis_base = DavisBase()
davis_base.commit()

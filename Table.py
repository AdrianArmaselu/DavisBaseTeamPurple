import math
import os

from Page import Page


class Table(Page):
    page_size = 512
    datePattern = "yyyy-MM-dd_HH:mm:ss"

    def __init__(self, table_name):
        self.table_name = table_name
        self.data_dir = os.path.join(os.getcwd(), 'data')
        self.table_dir = self.data_dir + "/" + self.table_name
        self.table_file_path = self.table_dir + "/" + self.table_name + ".tbl"
        self.dtype_bytes = {"null": 0, "tinyint": 2, "smallint": 2, "int": 4, "bigint": 8, "long": 8,
                            "float": 4, "double": 8, "year": 1, "time": 4, "datatime": 8, "date": 8}
        self.struct_format_string = {"null": "x", "tinyint": 'b', "smallint": 'h', "int": 'i',
                                     "bigint": "q", "long": "q", "float": "f", "double": "d", "year": "H", "time": "i",
                                     "datatime": "Q", "date": "Q"}

    # Create a table if the table already didn't exists
    def create_table(self, table_name):
        self.__init__(table_name)
        print(self.data_dir)
        if not os.path.isdir(self.data_dir):
            os.makedirs(self.table_dir)
        else:
            os.mkdir(self.table_dir)
        with open(self.table_file_path, 'wb') as f:
            print(self.table_file_path + " is created")
            return self.table_file_path

    # Check if the tale exist in the database already by checking the catalog
    def check_if_table_exists(self, table_path):
        return os.path.exists(table_path)

    def values_to_fstring(self, col_dtype, values):
        fstring = " "
        print(col_dtype, values)
        for dt in range(0, len(col_dtype)):
            if col_dtype[dt] in self.struct_format_string.keys():
                fstring += self.struct_format_string[col_dtype[dt]]
            elif col_dtype[dt] == "text":
                fstring += str(len(values[dt])) + "s"
            fstring += " "
        fstring = fstring.strip()
        return fstring

    def schema_to_fstring(self, col_dtype):
        fstring = " "
        for dt in range(0, len(col_dtype)):
            if col_dtype[dt] in self.struct_format_string.keys():
                fstring += self.struct_format_string[col_dtype[dt]]
            elif col_dtype[dt] == "text":
                fstring += "s"
        fstring = fstring.strip()
        return fstring

    def string_encoding(self, record):
        r_values = []
        for r in record:
            if type(r) is str:
                r_values.append(r.encode('utf-8'))
            else:
                r_values.append(r)
        return r_values

    def insert_into_table(self, table_name, values):
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
        print("Table is existing")
        root_node = self.get_root_node(self.table_file_path)
        print("returned root node is", root_node)
        record_payload = self.calculate_payload_size(values)
        # check the number of pages in the table
        if record_payload > 512:
            print("Record size is greater than 512 bytes..Cannot accommodate the record in the table")
        # Checking if the left-leaf node exists
        page_number = root_node[-2]
        page_last_rowid = root_node[-1]
        print("the page number is {0} and row is is {1}".format(str(page_number), str(page_last_rowid)))
        col_dtype, col_constraint = self.scheme_dtype_constraint()
        insert_success = False
        if len(root_node) == 2:
            if page_last_rowid == 0 and record_payload < 512:
                row_id = 1
                record = self.string_encoding([row_id] + values)
                page_offset = page_number * 512 + 1
                print("record is", record)
                fstring = self.values_to_fstring(col_dtype, record)
                print("Creating new record", page_number, page_offset, record, fstring)
                insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
                                                    record_payload)
                if insert_success:
                    self.update_root_node(self.table_file_path, [page_number, row_id])
            else:
                page_filled_size = self.check_page_size(self.table_file_path, page_number)
                print("Filled Page size", page_filled_size)
                page_size_availability = self.page_size - page_filled_size
                print("Page size availability", page_size_availability)
                if record_payload <= page_size_availability:
                    page_offset = (page_filled_size + 1) + page_number * 512
                    record = self.string_encoding([page_last_rowid + 1] + values)
                    fstring = self.values_to_fstring(col_dtype, record)
                    print("Creating new record", page_number, page_offset, record, fstring)
                    insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
                                                        record_payload)
                    if insert_success:
                        self.update_root_node(self.table_file_path, [page_number, page_last_rowid + 1])
                else:
                    print("Creating new page")
                    new_page_number = page_number + 1
                    new_page_rowid = page_last_rowid + 1
                    print("the new page no and rowid", new_page_number, new_page_rowid)
                    page_offset = new_page_number * 512 + 1
                    record = self.string_encoding([new_page_rowid] + values)
                    fstring = self.values_to_fstring(col_dtype, record)
                    print("Creating new record", page_number, page_offset, record, fstring)
                    insert_success = self.write_to_page(self.table_file_path, new_page_number, page_offset, record,
                                                        fstring, record_payload)
                    if insert_success:
                        self.update_root_node(self.table_file_path, [new_page_number, new_page_rowid])
        if insert_success:
            print("Record has been successfully added")
        else:
            print("Error occurred while adding new record")

    def traverse_tree(self, table_name):
        self.__init__(table_name)
        col_dtype, col_constraint = self.scheme_dtype_constraint()
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
        print("Table is existing")
        col_dtype, col_constraint = self.scheme_dtype_constraint()
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        page_records = []
        for page_no in range(0, len(root_node), 2):
            page_records.append(self.read_page(self.table_file_path, col_dtype, page_no, s_fstring))
        print(page_records)

    def calculate_payload_size(self, values):
        self.table_desc = {"col1": {"datatype": "int", "constraints": "pri:not null"},
                           "col2": {"datatype": "int", "constraints": "not null"},
                           "col2": {"datatype": "text", "constraints": "not null"},
                           "col3": {"datatype": "int", "constraints": ""}}
        # table_desc = self.table_dtype_constraint()
        dtype_bytes = []
        for index, (col, col_cons) in enumerate(self.table_desc.items()):
            if col_cons["datatype"] in self.dtype_bytes:
                dtype_bytes.append(self.dtype_bytes[col_cons["datatype"]])

            if col_cons["datatype"] == "text":
                dtype_bytes.append(len(values[index]))
        print(dtype_bytes)
        payload_size = sum(dtype_bytes)
        print("Payload size is", payload_size)
        return payload_size

    # get the datatype,constraints from the meta-data
    def scheme_dtype_constraint(self):
        self.table_desc = {"col1": {"datatype": "int", "constraints": "pri:not null"},
                           "col2": {"datatype": "int", "constraints": "not null"},
                           "col3": {"datatype": "text", "constraints": "not null"},
                           "col4": {"datatype": "float", "constraints": ""}}
        self.table_constraints = []
        self.table_dtypes = []
        for (col, col_cons) in self.table_desc.items():
            self.table_dtypes.append(col_cons["datatype"])
            self.table_constraints.append(col_cons["constraints"])
        return self.table_dtypes, self.table_constraints


Table = Table("sample")
# Table.create_table("sample")


# Table.insert_into_table("sample", [121, "kavin", 100])
# Table.insert_into_table("sample", [122, "abcdeafghijk", 99])
# Table.insert_into_table("sample", [123, "kuppusamy", 88.5])
# Table.insert_into_table("sample", [124, "ha", 40])

Table.traverse_tree("sample")
# print(Table.calculate_payload_size([1, "kavin", 100]))

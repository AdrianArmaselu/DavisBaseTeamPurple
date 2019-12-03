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
        self.accepted_operator = ["=", ">", ">=", "<", "<=", "<>"]

    # Create a table if the table already didn't exists
    def create_table(self, table_name):
        self.__init__(table_name)
        print(self.data_dir)
        try:
            if not os.path.isdir(self.data_dir):
                os.makedirs(self.table_dir)
            else:
                os.mkdir(self.table_dir)
            with open(self.table_file_path, 'wb') as f:
                print(self.table_name + " table is created")
                return self.table_file_path
        except FileExistsError:
            print("Table already exists..You cannot create the same table again!")

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

    def explicit_type_conv(self, col_dtype, values):
        if len(col_dtype) == len(values):
            for dt in range(0, len(col_dtype)):
                if col_dtype[dt] == "float":
                    values[dt] == float(values[dt])
                elif col_dtype[dt] == "int":
                    values[dt] == int(values[dt])
        else:
            print("Error while type conversion")

        return values

    def insert_into_table(self, table_name, values):
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please create a table first")
            return False
        print("Table is existing")
        root_node = self.get_root_node(self.table_file_path)
        print("returned root node is", root_node)
        for val in range(0, len(values)):
            if isinstance(values[val], str):
                values[val] = (values[val] + ">x")
        record_payload = self.calculate_payload_size([0] + values)
        # check the number of pages in the table
        if record_payload > 512:
            print("Record size is greater than 512 bytes..Cannot accommodate the record in the table")
        # Checking if the left-leaf node exists
        page_number = root_node[-3]
        page_total_record = root_node[-2]
        page_last_rowid = root_node[-1]
        print("total records in page", page_total_record)
        print("the page number is {0} and row is is {1}".format(str(page_number), str(page_last_rowid)))
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        insert_success = False
        if page_last_rowid == 0 and record_payload < 512:
            row_id = 1
            record = self.string_encoding([row_id] + values)
            page_offset = page_number * self.page_size
            print("record is", record)
            fstring = self.values_to_fstring(col_dtype, record)
            record = self.explicit_type_conv(col_dtype, record)
            print("Creating1 new record", page_number, page_offset, record, fstring)
            insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
                                                record_payload)
            if insert_success:
                root_node_len = len(root_node)
                if root_node_len == 3:
                    root_offset = 0
                self.update_root_node(self.table_file_path, [page_number, page_total_record + 1, row_id], root_offset)
        else:
            page_filled_size = self.check_page_size(self.table_file_path, page_number)
            print("Filled Page size", page_filled_size)
            page_size_availability = self.page_size - page_filled_size
            print("Page size availability", page_size_availability)
            if record_payload <= page_size_availability:
                page_offset = (page_filled_size) + page_number * self.page_size
                record = self.string_encoding([page_last_rowid + 1] + values)
                fstring = self.values_to_fstring(col_dtype, record)
                print("Creating2 new record", page_number, page_offset, record, fstring)
                record = self.explicit_type_conv(col_dtype, record)
                insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
                                                    record_payload)
                if insert_success:
                    root_node_len = len(root_node)
                    if root_node_len == 3:
                        root_offset = 0
                    else:
                        root_offset = ((root_node_len // 3) - 1) * 12
                    self.update_root_node(self.table_file_path,
                                          [page_number, page_total_record + 1, page_last_rowid + 1], root_offset)
            else:
                print("Creating new page")
                new_page_number = page_number + 1
                new_page_rowid = page_last_rowid + 1
                page_total_record = 1
                print("the new page no and rowid", new_page_number, new_page_rowid)
                page_offset = new_page_number * self.page_size
                record = self.string_encoding([new_page_rowid] + values)
                fstring = self.values_to_fstring(col_dtype, record)
                print("Creating3 new record", page_number, page_offset, record, fstring)
                record = self.explicit_type_conv(col_dtype, record)
                insert_success = self.write_to_page(self.table_file_path, new_page_number, page_offset, record,
                                                    fstring, record_payload)
                if insert_success:
                    root_offset = ((len(root_node)) // 3) * 12
                    self.update_root_node(self.table_file_path,
                                          [new_page_number, page_total_record, new_page_rowid], root_offset)
        if insert_success:
            print("Record has been successfully added")
            return True
        else:
            print("Error occurred while adding new record")
            return False

    def traverse_tree(self, table_name):
        self.__init__(table_name)

        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
            return False
        print("Table is existing")
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        print("root node is", root_node)
        page_records = []
        print(root_node)

        for page_no in range(0, len(root_node), 3):
            print("hello")
            print("for the page number", root_node[page_no], col_dtype, s_fstring)
            print("the number of records in the page is", root_node[page_no + 1])
            ret_val, record_val = self.read_page(self.table_file_path, col_dtype, int(root_node[page_no]), s_fstring,
                                                 root_node[page_no + 1])
            print("hello", record_val)
            if ret_val:
                page_records += record_val
            else:
                print("Error while traversing through Tree")
                break
        print("All the records in the page", page_records)
        return page_records

    def delete_record(self, table_name, column, operator, value, is_not=False):
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
            return False
        print("Table is existing")
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        if column not in column_names:
            return False
        if operator not in self.accepted_operator:
            return False
        column_index = column_names.index(column)
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        page_records = []
        print(root_node)
        total_deleted_records = []
        insert_success = False
        for page_no in range(0, len(root_node), 3):
            page_number = root_node[page_no]
            page_total_recs = root_node[page_no + 1]
            page_last_rid = root_node[page_no + 2]
            print("for the page number", page_number, col_dtype, s_fstring)
            print("the number of records in the page is", page_total_recs)
            ret_val, record_val = self.read_page(self.table_file_path, col_dtype, page_number, s_fstring,
                                                 page_total_recs)
            print("the val and record", ret_val, record_val, len(record_val))
            if ret_val:
                # new_page_records = []
                # for rec in range(0, len(record_val)):
                # print("the for loop", rec, record_val[rec], record_val[rec][column_index])
                deleted_records, new_page_records = self.column_condition_check(record_val, operator, value,
                                                                                column_index, is_not)
            else:
                print("Error while traversing through Tree")
                break
            print("new page records", new_page_records)
            print("deleted records", deleted_records)

            for dr in deleted_records:
                total_deleted_records.append(dr)

            if len(total_deleted_records) > 0:
                self.page_clean_bytes(self.table_file_path, page_number)
                page_offset = page_number * self.page_size
                for record in new_page_records:

                    for val in range(0, len(record)):
                        if isinstance(record[val], str):
                            record[val] = (record[val] + ">x")
                    record = self.string_encoding(record)
                    fstring = self.values_to_fstring(col_dtype, record)
                    record_payload = self.calculate_payload_size(record)
                    print("hey the record", record)
                    print("the fstring is ", col_dtype, record)
                    print("the record and payload", record, fstring, record_payload)
                    print("wrting to page", page_number, page_offset, record, fstring, record_payload)
                    record = self.explicit_type_conv(col_dtype, record)
                    insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
                                                        record_payload)
                if insert_success:
                    root_node_len = len(root_node)
                    if root_node_len == 3:
                        root_offset = 0
                    else:
                        root_offset = (page_no // 3) * 12
                    page_total_recs = len(new_page_records)
                    print("updating the root node", page_number, page_total_recs, page_last_rid, root_offset)
                    self.update_root_node(self.table_file_path, [page_number, page_total_recs, page_last_rid],
                                          root_offset)
                else:
                    print("Error while writing into page")
            else:
                print("no change in this page")
            print("deleted records", deleted_records)
        return True

    def calculate_payload_size(self, values):
        self.table_desc = {"col1": {"datatype": "int", "constraints": "pri:not null"},
                           "col2": {"datatype": "int", "constraints": "not null"},
                           "col3": {"datatype": "text", "constraints": "not null"},
                           "col4": {"datatype": "int", "constraints": ""}}
        # table_desc = self.table_dtype_constraint()
        dtype_bytes = []
        for index, (col, col_cons) in enumerate(self.table_desc.items()):
            if col_cons["datatype"] in self.dtype_bytes:
                dtype_bytes.append(self.dtype_bytes[col_cons["datatype"]])

            if col_cons["datatype"] == "text":
                # print("in payload", values[index], values)
                dtype_bytes.append(len(values[index]))
        # print(dtype_bytes)
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
        self.column_names = []
        for (col, col_cons) in self.table_desc.items():
            self.table_dtypes.append(col_cons["datatype"])
            self.table_constraints.append(col_cons["constraints"])
            self.column_names.append(col)
        return self.table_dtypes, self.table_constraints, self.column_names

    def column_condition_check(self, record_val, cond_operator, value, column_index, is_not=False):
        impacted_records = []
        unimpacted_records = []
        for rec in range(0, len(record_val)):
            print("the for loop", rec, record_val[rec], record_val[rec][column_index])
            if cond_operator == "=":
                if is_not:
                    if not record_val[rec][column_index] == value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] == value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == ">":
                if is_not:
                    if not record_val[rec][column_index] > value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] > value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == ">=":
                if is_not:
                    if not record_val[rec][column_index] >= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] >= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == "<":
                if is_not:
                    if not record_val[rec][column_index] < value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] < value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == "<=":
                if is_not:
                    if not record_val[rec][column_index] <= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] <= value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
            if cond_operator == "<>":
                if is_not:
                    if not record_val[rec][column_index] != value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])
                else:
                    if record_val[rec][column_index] > value:
                        impacted_records.append(record_val[rec])
                    else:
                        unimpacted_records.append(record_val[rec])

        return impacted_records, unimpacted_records

    def update_matched_records(self, updated_records, set_column, set_value, set_column_index):
        print(updated_records,len(updated_records))
        for rec in range(0, len(updated_records)):
            print(rec)
            print("update matahced records",updated_records[rec],set_value,set_column_index)
            updated_records[rec][set_column_index] = set_value
        print("returning the updated records",updated_records)
        return updated_records

    def update_record(self, table_name, set_column, set_value, cond_column, cond_operator, cond_value, is_not=False):
        self.__init__(table_name)
        table_exists = self.check_if_table_exists(self.table_file_path)
        if not table_exists:
            print(self.table_name + " is not exists in the DavisBase...Please check the table name")
            return False
        print("Table is existing")
        col_dtype, col_constraint, column_names = self.scheme_dtype_constraint()
        if set_column not in column_names and cond_column not in column_names:
            return False
        if cond_operator not in self.accepted_operator:
            return False
        set_column_index = column_names.index(set_column)
        cond_column_index = column_names.index(cond_column)
        s_fstring = self.schema_to_fstring(col_dtype)
        root_node = self.get_root_node(self.table_file_path)
        page_records = []
        print("root noe is", root_node)
        deleted_records = []
        root_last_rid = root_node[-1]
        move_records = []
        for page_no in range(0, len(root_node), 3):
            page_number = root_node[page_no]
            page_total_recs = root_node[page_no + 1]
            page_last_rid = root_node[page_no + 2]
            print("for the page number", page_number, col_dtype, s_fstring)
            print("the number of records in the page is", page_total_recs)
            ret_val, record_val = self.read_page(self.table_file_path, col_dtype, int(page_number), s_fstring,
                                                 page_total_recs)
            print("the val and record", ret_val, record_val, len(record_val))
            if ret_val:
                print("column condition checking",record_val, cond_operator, cond_value,cond_column_index, is_not)
                updated_records, n_page_records = self.column_condition_check(record_val, cond_operator, cond_value,
                                                                              cond_column_index, is_not)
            else:
                print("Error while traversing through Tree")
                break
            print("updated records are ", updated_records)
            print("Old records are ", n_page_records)
            if len(updated_records) > 0:
                print("checking for the udpated recorsd",updated_records, set_column, set_value, set_column_index)
                updated_records = self.update_matched_records(updated_records, set_column, set_value, set_column_index)
                records_size = 0

                for rec in n_page_records:
                    records_size += self.calculate_payload_size(rec)
                for rec in updated_records:
                    rec_size = self.calculate_payload_size(rec)
                    if (records_size + rec_size) < self.page_size:
                        n_page_records.append(rec)
                        records_size = records_size + rec_size
                    else:
                        move_records.append(rec)

                #self.page_clean_bytes(self.table_file_path, page_number)
                page_offset = page_number * self.page_size
                for record in n_page_records:
                    for val in range(0, len(record)):
                        if isinstance(record[val], str):
                            record[val] = (record[val] + ">x")
                    record = self.string_encoding(record)
                    fstring = self.values_to_fstring(col_dtype, record)
                    record_payload = self.calculate_payload_size(record)
                    print("hey the record", record)
                    print("the fstring is ", col_dtype, record)
                    print("the record and payload", record, fstring, record_payload)
                    print("wrting to page", page_number, page_offset, record, fstring, record_payload)
                    insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
                                                        record_payload)
                if insert_success:
                    root_node_len = len(root_node)
                    if root_node_len == 3:
                        root_offset = 0
                    else:
                        root_offset = (page_no // 3) * 12
                    page_total_recs = len(n_page_records)
                    print("updaing the root node", page_number, page_total_recs, page_last_rid, root_offset)
                    self.update_root_node(self.table_file_path, [page_number, page_total_recs, page_last_rid],
                                          root_offset)
                    print("updated root node is",self.get_root_node(self.table_file_path))
                else:
                    print("Error while writing into page")
            else:
                print("no change in this page")
                continue
        for record in range(len(move_records)):
            '''
            for val in range(0, len(record)):
                if isinstance(record[val], str):
                    record[val] = (record[val] + ">x")
            '''
            self.insert_into_table(self.table_name, record[1:])
            
            # Table.insert_into_table("sample", [123, "kuppusamy>x", 88.5])
            # insert_success = self.write_to_page(self.table_file_path, page_number, page_offset, record, fstring,
            # record_payload)
        print("updated records are",move_records)
        print("updated root node at end",self.get_root_node(self.table_file_path))
        return True


Table = Table("sample")
Table.create_table("sample")

Table.insert_into_table("sample", [121, "kavin", 100])
Table.insert_into_table("sample", [122, "abcdeafghijk", 99])
Table.insert_into_table("sample", [123, "kuppusamy", 88.5])
Table.insert_into_table("sample", [124, "", 40])
Table.traverse_tree("sample")

column = "col2"
operator = ">="
value = 124
is_not = True
Table.delete_record("sample", column, operator, value, is_not)
Table.traverse_tree("sample")
Table.insert_into_table("sample", [123, "kuppusamy", 88.5])
Table.traverse_tree("sample")

set_column = "col2"
set_value = 125
cond_column = "col4"
cond_operator = "<"
cond_value = 50.0
is_not = False

Table.update_record("sample", set_column, set_value, cond_column, cond_operator, cond_value, is_not)
Table.traverse_tree("sample")

set_column = "col3"
set_value = "kavin"
cond_column = "col4"
cond_operator = "<"
cond_value = 50.0
is_not = False

Table.update_record("sample", set_column, set_value, cond_column, cond_operator, cond_value, is_not)
Table.traverse_tree("sample")


#Table.select_from_table("sample",)



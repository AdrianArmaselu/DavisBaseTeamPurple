import unittest

from core.model import DavisBase, Record, LeafCell, TableLeafPage, DavisTable, TableColumnsMetadata, UpdateArgs, \
    Condition, SelectArgs, DeleteArgs, ColumnDefinition
from core.datum import Null, TinyInt, SmallInt, Int, Long, Float, Double, Year, Time, DateTime, Date, Text


class FileIoTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(FileIoTests, self).__init__(*args, **kwargs)
        # self.davis_base = DavisBase()

    def test_record(self):
        # empty values tests
        assert bytes(Record([Null()])) == b'\x01\x00'
        assert bytes(Record([TinyInt(0)])) == b'\x01\x01\x00'
        assert bytes(Record([SmallInt(0)])) == b'\x01\x02\x00\x00'
        assert bytes(Record([Int(0)])) == b'\x01\x03\x00\x00\x00\x00'
        assert bytes(Record([Long(0)])) == b'\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00'
        assert bytes(Record([Float(0)])) == b'\x01\x05\x00\x00\x00\x00'
        assert bytes(Record([Double(0)])) == b'\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00'
        assert bytes(Record([Year(0)])) == b'\x01\x07\x00'
        assert bytes(Record([Time(0)])) == b'\x01\x08\x00\x00\x00\x00'
        assert bytes(Record([DateTime(0)])) == b'\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00'
        assert bytes(Record([Date(0)])) == b'\x01\x0A\x00\x00\x00\x00\x00\x00\x00\x00'
        assert bytes(Record([Text('')])) == b'\x01\x0B'

        # mixed
        assert bytes(Record([Null(), Int(9), Text('hello')])) == b'\x03\x00\x03\x10\x00\x00\x00\x09hello'

        # get
        assert Record([Null(), Int(9), Text('hello')])[1] == Int(9)
        # print(Record([Null(), Int(9), Text('hello')])[1])

    def test_update_record(self):
        Record([Null(), Int(9), Text('hello')]).set(1, Int(8))
        value = Record([Null(), Int(9), Text('hello')])
        value.set(1, Int(8))
        assert str(value) == "['NULL', '8', 'hello']"

    def test_leaf_cell(self):
        cell = LeafCell(0, Record([Null(), Int(9), Text('hello')]))
        cell.set(1, Int(8))
        print(bytes(cell))

    def test_leaf_page(self):
        cell0 = LeafCell(0, Record([Null(), Int(9), Text('hello')]))
        cell1 = LeafCell(1, Record([Null(), Int(10), Text('howdy')]))
        page = TableLeafPage(0, 0, {0: cell0})
        print("before", page)
        page.insert(1, cell1)
        print("insert", page)
        page.update(UpdateArgs(1, Int(10), Condition(1, "=", Int(9))))
        result = page.select(SelectArgs([1], Condition(1, "=", Int(11))))
        for r in result:
            print([str(a) for a in r])
        print("update", page)
        page.delete(DeleteArgs(Condition(1, "=", Int(10))))
        print("delete", page)
        # check bytes
        pass

    def test_table(self):
        cell0 = LeafCell(0, Record([Int(0), Text('t1')]))
        cell1 = LeafCell(1, Record([Int(1), Text('t2')]))

        table = DavisTable("test", 1,
                           TableColumnsMetadata(
                               {"rowid": ColumnDefinition("INT", 0), "table_name": ColumnDefinition("TEXT", 1)}),
                           [TableLeafPage(0, 0, {0: cell0})])

        print(table)
        print(bytes(table))
        table.insert([[2, 't3']])
        print(table)
        table.update('table_name', "t4", "rowid", "=", "0")
        print(table)
        table.delete("table_name", "=", "t2")
        print(table)
        select = table.select(['table_name', 'rowid'], "rowid", ">=", "0")

        print([str(f) for f in select])
        print(bytes(table))
        # write empty page
        # write page with one record and one column
        # write page with one record and multiple columns
        # write page with multiple records and multiple columns
        # write multiple pages and cover same scenarios as above
        self.assertEqual(True, False)

    def test_davis_base(self):
        davis_base = DavisBase()
        print(davis_base.show_tables())
        # davis_base.create_table('test', TableColumnsMetadata(
        #     {"rowid": ColumnDefinition("INT", 0), "table_name": ColumnDefinition("TEXT", 1)}))
        # davis_base.insert('test', ['0', 't1'])
        #
        # result = davis_base.select("test", ['rowid', 'table_name'], "rowid", ">=", "0")
        # for r in result:
        #     print([str(c) for c in r])
        #
        # davis_base.commit()
        #
        # new_base = DavisBase()
        # new_base = davis_base.select("test", ['rowid', 'table_name'], "rowid", ">=", "0")
        # for r in result:
        #     print([str(c) for c in r])


        pass

    def test_davis_base_read(self):
        davis_base = DavisBase()


    def empty_database_init(self):
        pass


if __name__ == '__main__':
    unittest.main()

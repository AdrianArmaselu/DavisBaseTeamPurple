import unittest


class FileIoTests(unittest.TestCase):
    def table_empty_page(self):
        # write empty page
        # write page with one record and one column
        # write page with one record and multiple columns
        # write page with multiple records and multiple columns
        # write multiple pages and cover same scenarios as above
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()

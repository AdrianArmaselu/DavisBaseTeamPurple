
def read_table(table_name):
    f = open(table_name, "rb")
    f.seek(0, 0)

    def does_page_exist_at_position(p):
        f.seek(p)
        return read_int(1) in [2, 5, 10, 13]

    def skip_bytes(n):
        f.seek(n, 1)

    def to_int(number_bytes):
        return int(number_bytes.hex(), 16)

    def read_int(size):
        return to_int(f.read(size))

    def read_page_header():
        page_type = read_int(1)
        print('page_type: ', page_type)

        skip_bytes(1)

        number_of_cells = read_int(2)
        print('number_of_cells: ', number_of_cells)

        content_area_offset = read_int(2)
        print('content_area_offset: ', content_area_offset)

        node_page_number = read_int(4)
        print('node_page_number: ', node_page_number)

        page_parent = read_int(4)
        print('page_parent: ', page_parent)

        skip_bytes(2)

        cell_offsets = [read_int(2) for c in range(number_of_cells)]

        return page_type, number_of_cells, content_area_offset, node_page_number, page_parent, cell_offsets

    def read_cell():
        # f.seek(page_position, )

        payload_size = read_int(2)
        row_id = read_int(4)
        number_of_columns = read_int(1)
        data_types = [read_int(1) for c in range(number_of_columns)]


        def read_column(data_type):
            return {
                0: 0,
                1: 1,
                2: 2,
                3: 4,
                4: 8,
                5: 4,
                6: 8,
                8: 1,
                9: 4,
                10: 8,
                11: 8
                # 12-115 represents number of bytes for string, so return self if within 12-115 range
            }


        return 1

    page_position = 0
    pages = []
    while does_page_exist_at_position(page_position):
        f.seek(page_position)
        page_type, number_of_cells, content_area_offset, node_page_number, page_parent, cell_offsets = read_page_header()
        cells = [read_cell() for offset in cell_offsets]
        pages.append((page_type, number_of_cells, node_page_number, page_parent, cells))
        page_position += 512

    f.close()

    return pages


read_table("table")

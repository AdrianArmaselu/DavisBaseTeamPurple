from typing import AnyStr, List

from core.datum import Null, TinyInt, SmallInt, Int, Long, Float, Double, Year, Time, DateTime, Date, Text

IS_DEBUG_LOGGING_ENABLED = False


def disable_debugging_logs():
    global IS_DEBUG_LOGGING_ENABLED
    IS_DEBUG_LOGGING_ENABLED = False


def log_debug(*values):
    if IS_DEBUG_LOGGING_ENABLED:
        print("DEBUG TableFile -", *values)


DATA_TYPES = {
    0: Null,
    1: TinyInt,
    2: SmallInt,
    3: Int,
    4: Long,
    5: Float,
    6: Double,
    7: Year,
    8: Time,
    9: DateTime,
    10: Date,
    11: Text,
}

DATA_TYPE_NAMES = {
    0: "NULL",
    1: "TINYINT",
    2: "SMALLINT",
    3: "INT",
    4: "BIGINT",
    5: "FLOAT",
    6: "DOUBLE",
    7: "YEAR",
    8: "TIME",
    9: "DATETIME",
    10: "DATE",
    11: "TEXT",
}

for i in range(12, 115):
    DATA_TYPES[i] = Text
    DATA_TYPE_NAMES[i] = "TEXT"

data_type_encodings = {v: k for k, v in DATA_TYPE_NAMES.items()}


def is_int(data_type: int) -> bool:
    return 0 < DATA_TYPE_NAMES[data_type] < 5


def flatten(input_list: List) -> List:
    return [item for sublist in input_list for item in sublist]


def bytes_to_int(number_bytes: AnyStr) -> int:
    return int.from_bytes(number_bytes, 'big')


def int_to_bytes(number: int, size: int = 4) -> AnyStr:
    return int.to_bytes(number, size, 'big')


def get_column_size(column_type: int) -> int:
    return {0: 0, 1: 1, 2: 2, 3: 4, 4: 8, 5: 4, 6: 8, 7: 1, 8: 4, 9: 8, 10: 8}[column_type] \
        if column_type < 11 else column_type - 11


def value_to_bytes(value: str or int, value_byte_size: int) -> AnyStr:
    return bytes(value, 'utf-8') if isinstance(value, str) else int_to_bytes(value, value_byte_size)


def leaf_cell_header_size():
    return 2 + 4


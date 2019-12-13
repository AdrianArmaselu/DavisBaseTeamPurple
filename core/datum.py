# abstract type
import datetime
import struct


class DavisBaseType:
    def __init__(self, value: int or str or bytes = None):
        self.value: int or str or bytes = value

    def get_type_number(self) -> int:
        pass

    def __len__(self) -> int:
        pass

    def __bytes__(self) -> bytes:
        pass


class Null(DavisBaseType):

    def get_type_number(self) -> int:
        return 0

    def __eq__(self, other: 'Null') -> bool:
        return isinstance(other, Null)

    def __ne__(self, other: 'Null') -> bool:
        return not isinstance(other, Null)

    def __gt__(self, other: 'Null') -> bool:
        raise RuntimeError('Cannot compare with NULL value')

    def __ge__(self, other: 'Null') -> bool:
        raise RuntimeError('Cannot compare with NULL value')

    def __lt__(self, other: 'Null') -> bool:
        raise RuntimeError('Cannot compare with NULL value')

    def __le__(self, other: 'Null') -> bool:
        raise RuntimeError('Cannot compare with NULL value')

    def __len__(self) -> int:
        return 0

    def __bytes__(self) -> bytes:
        return b''

    def __str__(self) -> str:
        return 'NULL'


class DavisBaseTypeComparable(DavisBaseType):

    def __eq__(self, other: 'DavisBaseTypeComparable') -> bool:
        return self.value == other.value

    def __ne__(self, other: 'DavisBaseTypeComparable') -> bool:
        return self.value != other.value

    def __gt__(self, other: 'DavisBaseTypeComparable') -> bool:
        return self.value > other.value

    def __ge__(self, other: 'DavisBaseTypeComparable') -> bool:
        return self.value >= other.value

    def __lt__(self, other: 'DavisBaseTypeComparable') -> bool:
        return self.value < other.value

    def __le__(self, other: 'DavisBaseTypeComparable') -> bool:
        return self.value <= other.value

    def __add__(self, other: 'DavisBaseTypeComparable') -> int or float:
        return self.value + other.value

    def __sub__(self, other: 'DavisBaseTypeComparable') -> int or float:
        return self.value - other.value

    def __mul__(self, other: 'DavisBaseTypeComparable') -> int or float:
        return self.value * other.value

    def __truediv__(self, other: 'DavisBaseTypeComparable') -> int or float:
        return self.value / other.value


class Number(DavisBaseTypeComparable):

    def __init__(self, value: int or float or bytes or str):
        super(Number, self).__init__(value)
        if isinstance(value, bytes):
            self.value: int or float = int.from_bytes(value, 'big', signed=True)
        if isinstance(value, str):
            self.value: int or float = self.from_str(value)
        self.__bytes__()  # try and build the bytes to see if it is possible

    def from_str(self, value: str) -> int:
        return int(value)

    # abstract
    def __len__(self) -> int:
        pass

    # abstract
    def __bytes__(self) -> bytes:
        pass

    def __str__(self) -> str:
        return str(self.value)


class Int(Number):
    def get_type_number(self) -> int:
        return 3

    def __len__(self) -> int:
        return 4

    def __bytes__(self) -> bytes:
        return int.to_bytes(self.value, len(self), 'big', signed=True)


class TinyInt(Int):
    def get_type_number(self) -> int:
        return 1

    def __len__(self) -> int:
        return 1


class SmallInt(Int):
    def get_type_number(self) -> int:
        return 2

    def __len__(self) -> int:
        return 2


class Long(Int):
    def get_type_number(self) -> int:
        return 4

    def __len__(self) -> int:
        return 8


class Float(Number):
    def __init__(self, value: int or float or bytes or str):
        super(Number, self).__init__(value)
        if isinstance(value, bytes):
            v= struct.unpack('f', value)
            self.value: float = v[0]

    def from_str(self, value: str) -> float:
        return float(value)

    def get_type_number(self) -> int:
        return 5

    def __len__(self) -> int:
        return 4

    def __bytes__(self) -> bytes:
        return struct.pack('f', self.value)


class Double(Number):

    def __init__(self, value: int or float or bytes or str):
        super(Number, self).__init__(value)
        if isinstance(value, bytes):
            self.value: int or float = struct.unpack('d', value)

    def get_type_number(self) -> int:
        return 6

    def __len__(self) -> int:
        return 8

    def __bytes__(self) -> bytes:
        return struct.pack('d', self.value)


class Year(TinyInt):
    def get_type_number(self) -> int:
        return 7

    def __str__(self) -> str:
        return str(2000 + self.value)


class Time(Int):
    def get_type_number(self) -> int:
        return 8


class DateTime(Long):
    def get_type_number(self) -> int:
        return 9

    def __str__(self) -> str:
        return datetime.datetime.fromtimestamp(self.value).strftime('%Y-%m-%d_%H:%M:%S')


class Date(Long):
    def get_type_number(self) -> int:
        return 10

    def __str__(self) -> str:
        return datetime.datetime.fromtimestamp(self.value).strftime('%Y-%m-%d')


class Text(DavisBaseTypeComparable):
    def __init__(self, value: str or bytes):
        super(Text, self).__init__(value)
        if isinstance(value, bytes):
            self.value: str = value.decode("utf-8")

    def get_type_number(self) -> int:
        return 11 + len(self)

    def __len__(self):
        return len(self.value)

    def __bytes__(self):
        return bytes(self.value, 'utf-8')

    def __str__(self) -> str:
        return self.value

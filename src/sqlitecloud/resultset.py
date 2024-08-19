from enum import Enum
from typing import Any, Dict, List, Optional


class SQLITECLOUD_VALUE_TYPE(Enum):
    INTEGER = "INTEGER"
    FLOAT = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"
    NULL = "NULL"


class SQLITECLOUD_RESULT_TYPE(Enum):
    RESULT_OK = 0
    RESULT_ERROR = 1
    RESULT_STRING = 2
    RESULT_INTEGER = 3
    RESULT_FLOAT = 4
    RESULT_ROWSET = 5
    RESULT_ARRAY = 6
    RESULT_NONE = 7
    RESULT_JSON = 8
    RESULT_BLOB = 9


class SQLiteCloudResult:
    def __init__(
        self, tag: SQLITECLOUD_RESULT_TYPE, result: Optional[any] = None
    ) -> None:
        self.tag: SQLITECLOUD_RESULT_TYPE = tag
        self.nrows: int = 0
        self.ncols: int = 0
        self.version: int = 0
        # table values are stored in 1-dimensional array
        self.data: List[Any] = []
        self.colname: List[str] = []
        self.decltype: List[str] = []
        self.dbname: List[str] = []
        self.tblname: List[str] = []
        self.origname: List[str] = []
        self.notnull: List[str] = []
        self.prikey: List[str] = []
        self.autoinc: List[str] = []

        self.is_result: bool = False

        if result is not None:
            self.init_data(result)

    def init_data(self, result: any) -> None:
        self.nrows = 1
        self.ncols = 1
        self.data = [result]
        self.is_result = True

    def _compute_index(self, row: int, col: int) -> int:
        if row < 0 or row >= self.nrows:
            return -1
        if col < 0 or col >= self.ncols:
            return -1
        return row * self.ncols + col

    def get_value(self, row: int, col: int) -> Optional[any]:
        index = self._compute_index(row, col)
        if index < 0 or not self.data or index >= len(self.data):
            return None

        return self.data[index]

    def get_name(self, col: int) -> Optional[str]:
        if col < 0 or col >= self.ncols:
            return None
        return self.colname[col]

    def get_decltype(self, col: int) -> Optional[str]:
        if col < 0 or col >= self.ncols or col >= len(self.decltype):
            return None

        return self.decltype[col]


class SQLiteCloudResultSet:
    def __init__(self, result: SQLiteCloudResult) -> None:
        self._iter_row: int = 0
        self._result: SQLiteCloudResult = result

    def __getattr__(self, attr: str) -> Optional[Any]:
        return getattr(self._result, attr)

    def __iter__(self):
        return self

    def __next__(self):
        if self._result.data and self._iter_row < self._result.nrows:
            out: Dict[str, any] = {}

            if self._result.is_result:
                out = {"result": self.get_value(0, 0)}
                self._iter_row += 1
            else:
                for col in range(self._result.ncols):
                    out[self.get_name(col)] = self.get_value(self._iter_row, col)
                self._iter_row += 1

            return out

        raise StopIteration

    def get_value(self, row: int, col: int) -> Optional[any]:
        return self._result.get_value(row, col)

    def get_name(self, col: int) -> Optional[str]:
        return self._result.get_name(col)

    def get_result(self) -> Optional[any]:
        return self.get_value(0, 0)


class SQLiteCloudOperationResult:
    """Result of a DML operation in a SQLite statement."""

    def __init__(self, result: SQLiteCloudResult) -> None:
        self._result = result

    @property
    def type(self) -> int:
        return self._result.data[0][0]

    @property
    def index(self) -> int:
        return self._result.data[0][1]

    @property
    def rowid(self) -> int:
        return self._result.data[0][2]

    @property
    def changes(self) -> int:
        return self._result.data[0][3]

    @property
    def total_changes(self) -> int:
        return self._result.data[0][4]

    @property
    def finalized(self) -> int:
        return self._result.data[0][5]

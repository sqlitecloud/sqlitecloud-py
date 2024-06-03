from typing import Any, Dict, List, Optional

from sqlitecloud.types import SQLITECLOUD_RESULT_TYPE, SQLITECLOUD_VALUE_TYPE


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

    def get_value(self, row: int, col: int, convert: bool = True) -> Optional[any]:
        index = self._compute_index(row, col)
        if index < 0 or not self.data or index >= len(self.data):
            return None

        value = self.data[index]
        return self._convert(value, col) if convert else value

    def get_name(self, col: int) -> Optional[str]:
        if col < 0 or col >= self.ncols:
            return None
        return self.colname[col]

    def _convert(self, value: str, col: int) -> any:
        if col < 0 or col >= len(self.decltype):
            return value

        decltype = self.decltype[col]
        if decltype == SQLITECLOUD_VALUE_TYPE.INTEGER.value:
            return int(value)
        if decltype == SQLITECLOUD_VALUE_TYPE.FLOAT.value:
            return float(value)
        if decltype == SQLITECLOUD_VALUE_TYPE.BLOB.value:
            # values are received as bytes before being strings
            return bytes(value)
        if decltype == SQLITECLOUD_VALUE_TYPE.NULL.value:
            return None

        return value


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

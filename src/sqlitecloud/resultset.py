from typing import Any, Dict, List, Optional


class SQCloudResult:
    def __init__(self, result: Optional[any] = None) -> None:
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


class SqliteCloudResultSet:
    def __init__(self, result: SQCloudResult) -> None:
        self._iter_row: int = 0
        self._result: SQCloudResult = result

    def __getattr__(self, attr: str) -> Any:
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

    def _compute_index(self, row: int, col: int) -> int:
        if row < 0 or row >= self._result.nrows:
            return -1
        if col < 0 or col >= self._result.ncols:
            return -1
        return row * self._result.ncols + col

    def get_value(self, row: int, col: int) -> Optional[any]:
        index = self._compute_index(row, col)
        if index < 0 or not self._result.data or index >= len(self._result.data):
            return None
        return self._result.data[index]

    def get_name(self, col: int) -> Optional[str]:
        if col < 0 or col >= self._result.ncols:
            return None
        return self._result.colname[col]

    def get_result(self) -> Optional[any]:
        return self.get_value(0, 0)

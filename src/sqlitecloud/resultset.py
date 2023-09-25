from typing import Dict
from sqlitecloud.driver import (
    SQCloudResultIsError,
    SQCloudRowsetCols,
    SQCloudRowsetColumnName,
    SQCloudRowsetInt32Value,
    SQCloudRowsetFloatValue,
    SQCloudRowsetRows,
    SQCloudRowsetValue,
    SQCloudRowsetValueType,
)
from sqlitecloud.wrapper_types import SQCLOUD_VALUE_TYPE, SQCloudResult


class SqliteCloudResultSet:
    _result = SQCloudResult

    def __init__(self, result: SQCloudResult) -> None:
        print("is ok", SQCloudResultIsError(result))  # TODO

        self._result = result
        self.row = 0
        self.rows = SQCloudRowsetRows(result)
        self.cols = SQCloudRowsetCols(self._result)
        self.col_names = list(
            SQCloudRowsetColumnName(self._result, i) for i in range(self.cols)
        )
        print(self.col_names)

    def __iter__(self):
        return self

    def __next__(self):
        if self.row < self.rows:
            out: Dict[str, any] = {}  # todo convert type
            for col in range(self.cols):
                # print("\t", col, self.col_names[col] )
                col_type = SQCloudRowsetValueType(
                    self._result, self.row, col
                ).value  # TODO memoize

                data = self._resolve_type(col, col_type)
                out[self.col_names[col]] = data
            self.row += 1
            return out
        raise StopIteration

    def _resolve_type(self, col, col_type):
        match col_type:
            case SQCLOUD_VALUE_TYPE.VALUE_INTEGER:
                return SQCloudRowsetInt32Value(self._result, self.row, col)
            case SQCLOUD_VALUE_TYPE.VALUE_FLOAT:
                return SQCloudRowsetFloatValue(self._result, self.row, col)
            case SQCLOUD_VALUE_TYPE.VALUE_TEXT:
                return SQCloudRowsetValue(self._result, self.row, col)
            case SQCLOUD_VALUE_TYPE.VALUE_BLOB:
                return SQCloudRowsetValue(self._result, self.row, col)

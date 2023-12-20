from typing import Any, Callable, Dict, List, Optional
from sqlitecloud.driver import (
    SQCloudResultFloat,
    SQCloudResultFree,
    SQCloudResultInt32,
    SQCloudResultIsError,
    SQCloudResultType,
    SQCloudRowsetCols,
    SQCloudRowsetColumnName,
    SQCloudRowsetInt32Value,
    SQCloudRowsetFloatValue,
    SQCloudRowsetRows,
    SQCloudRowsetValue,
    SQCloudRowsetValueType,
)
from sqlitecloud.wrapper_types import (
    SQCLOUD_VALUE_TYPE,
    SQCLOUD_RESULT_TYPE,
    SQCloudResult,
)


class SqliteCloudResultSet:
    _result: Optional[SQCloudResult] = None
    _data: List[Dict[str, Any]] = []

    def __init__(self, result: SQCloudResult) -> None:
        rs_type = SQCloudResultType(
            result,
        )
        match rs_type:
            case SQCLOUD_RESULT_TYPE.RESULT_ROWSET:
                self._init_resultset(result)
            case SQCLOUD_RESULT_TYPE.RESULT_OK:
                self.init_data(result, self._extract_ok_data)
            case SQCLOUD_RESULT_TYPE.RESULT_ERROR:
                self.init_data(result, self._extract_error_data)
            case SQCLOUD_RESULT_TYPE.RESULT_FLOAT:
                self.init_data(result, self._extract_float_data)
            case SQCLOUD_RESULT_TYPE.RESULT_INTEGER:
                self.init_data(result, self._extract_int_data)

    def init_data(
        self,
        result: SQCloudResult,
        extract_fn: Callable[[SQCloudResult], List[Dict[str, Any]]],
    ):
        self.row = 0
        self.rows = 1
        self._data = extract_fn(result)

    def _extract_ok_data(self, result: SQCloudResult):
        return [{"result": not SQCloudResultIsError(result)}]

    def _extract_error_data(self, result: SQCloudResult):
        return [{"result": SQCloudResultIsError(result)}]

    def _extract_float_data(self, result: SQCloudResult):
        return [{"result": SQCloudResultFloat(result)}]

    def _extract_int_data(self, result: SQCloudResult):
        return [{"result": SQCloudResultInt32(result)}]

    def _init_resultset(self, result):
        self._result = result
        self.row = 0
        self.rows = SQCloudRowsetRows(result)
        self.cols = SQCloudRowsetCols(self._result)
        self.col_names = list(
            SQCloudRowsetColumnName(self._result, i) for i in range(self.cols)
        )

    def __iter__(self):
        return self

    def __next__(self):
        if self._result:
            if self.row < self.rows:
                out: Dict[str, any] = {}
                for col in range(self.cols):
                    col_type = SQCloudRowsetValueType(
                        self._result, self.row, col
                    ).value

                    data = self._resolve_type(col, col_type)
                    out[self.col_names[col]] = data
                self.row += 1
                return out
        elif self._data:
            if self.row < self.rows:
                out: Dict[str, any] = self._data[self.row]
                self.row += 1
                return out

        SQCloudResultFree(self._result)
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

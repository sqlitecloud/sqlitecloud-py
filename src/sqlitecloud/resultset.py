from typing import Dict
from sqlitecloud.driver import SQCloudResultIsError
from sqlitecloud.wrapper_types import SQCloudResult


class SqliteCloudResultSet:
    _result = SQCloudResult

    def __init__(self, result: SQCloudResult) -> None:
        print("is ok", SQCloudResultIsError(result))  # TODO

        self._result = result
        self.row = 0

    def __iter__(self):
        return self

    def __next__(self):
        print(
            "row",
            self.row,
            "on",
            self._result.contents.num_rows,
            self._result.contents.num_columns,
        )
        if self.row < self._result.contents.num_rows:
            out: Dict[str, any] = {}  # todo convert type
            for col in range(self._result.contents.num_columns):
                print("\t", col)
                data = self._result.contents.data[
                    self.row * self._result.contents.num_columns + col
                ].decode("utf-8")
                column_name = self._result.contents.column_names[col].decode("utf-8")
                out[column_name] = data
            self.row += 1
            return out
        raise StopIteration

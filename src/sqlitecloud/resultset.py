from typing import Dict
from sqlitecloud.driver import SQCloudResultIsError, SQCloudRowsetCols, SQCloudRowsetColumnName, SQCloudRowsetRows
from sqlitecloud.wrapper_types import SQCloudResult




class SqliteCloudResultSet:
    _result = SQCloudResult

    def __init__(self, result: SQCloudResult) -> None:
        print("is ok", SQCloudResultIsError(result))  # TODO

        self._result = result
        self.row = 0
        self.rows = SQCloudRowsetRows(result)
        self.cols = SQCloudRowsetCols(self._result)
        self.col_names = list([SQCloudRowsetColumnName(self._result,i) for i in range(self.cols) ])
        print(self.rows, self.cols,len(self.col_names),'\n' ,self.col_names,'\n----------------------\n')


    def __iter__(self):
        return self

    def __next__(self):
        



        if self.row < self.rows:
            out: Dict[str, any] = {}  # todo convert type
            for col in range(self.cols):
                #print("\t", col, self.col_names[col] )
                data = self._result.contents.data[
                    self.row * self.cols + col
                ]
                column_name = self.col_names[col]
                out[column_name] = data
            self.row += 1
            return out
        raise StopIteration

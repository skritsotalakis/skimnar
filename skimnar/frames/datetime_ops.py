import narwhals as nw
from narwhals.dataframe import DataFrame
from skimnar.base_ops import BaseFrame


class DatetimeFrame(BaseFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df, "datetime")

    def _is_sorted(self, col_name: str) -> DataFrame:
        new_dict = {"sorted": [self.df[col_name].is_sorted()]}
        return nw.from_dict(new_dict, backend=self.df.implementation)

    def concat_horizontal(self, col_name: str) -> DataFrame:
        col = nw.col(col_name)
        return nw.concat(
            [
                self.df.select(col.null_count().alias("Nulls")),
                self.df.select(
                    (col.null_count() * 100 / nw.lit(self.df.shape[0])).alias("Null%")
                ),
                self.df.select(col.min().alias("min")),
                self.df.select(col.max().alias("max")),
                self._is_sorted(col_name),
            ],
            how="horizontal",
        )

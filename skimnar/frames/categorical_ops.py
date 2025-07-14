import narwhals as nw
from narwhals.dataframe import DataFrame
from skimnar.base_ops import BaseFrame


class CategoricalFrame(BaseFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df, "categorical")

    def _ordered_frame(self, col_name: str) -> DataFrame:
        series = self.df[col_name]
        return nw.from_dict(
            {"ordered": [nw.is_ordered_categorical(series)]},
            backend=series.implementation,
        )

    def concat_horizontal(self, col_name: str) -> DataFrame:
        col = nw.col(col_name)
        return nw.concat(
            [
                self.df.select(col.null_count().alias("Nulls")),
                self.df.select(
                    (col.null_count() * 100 / nw.lit(self.df.shape[0]))
                    .round(2)
                    .alias("Null%")
                ),
                self.df.select(col.n_unique().alias("Unique")),
                self._ordered_frame(col_name),
            ],
            how="horizontal",
        )

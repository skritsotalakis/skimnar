import narwhals as nw
from skimnar.base_ops import BaseFrame
import os
from narwhals.dataframe import DataFrame


class BooleanFrame(BaseFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df, "boolean")
        self.table = self.frame_to_table(self.frame_type.upper())

    def value_frequency_hist(
        self, col_name: str, hist_bins: int | None = None
    ) -> DataFrame:
        if hist_bins is None:
            hist_bins = 4

        start = ord("\u2581")
        unicode_labels = [" "] + [chr(start + i) for i in range(hist_bins - 1)]

        if os.name != "posix":
            unicode_labels[1] = "▃"
            unicode_labels[hist_bins - 1] = "▆"

        series = self.df[col_name].drop_nulls()

        if not series.dtype.is_numeric():
            series = series.cast(nw.Int64)

        hist_counts = series.hist(bin_count=hist_bins, include_breakpoint=False)
        pct_counts = hist_counts["count"] / series.shape[0]

        max_pct = pct_counts.max()
        if max_pct > 0:
            bin_labels = (pct_counts * (hist_bins - 1) / max_pct).round(0).cast(nw.Int64)
        else:
            bin_labels = pct_counts.cast(nw.Int64)

        unicode_hist_str = "".join([unicode_labels[label] for label in bin_labels])
        return nw.from_dict({"hist": [unicode_hist_str]}, backend=self.df.implementation)

    def concat_horizontal(self, col_name: str) -> DataFrame:
        col = nw.col(col_name)
        return nw.concat(
            [
                self.df.select(col.sum().alias("True")),
                self.df.select(
                    (col.sum() * 100 / nw.lit(self.df.shape[0])).round(2).alias("True%")
                ),
                self.value_frequency_hist(col_name),
            ],
            how="horizontal",
        )

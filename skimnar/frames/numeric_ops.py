import narwhals as nw
from skimnar.base_ops import BaseFrame
import os
from narwhals.dataframe import DataFrame


class NumericFrame(BaseFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df, "numeric")

    def value_frequency_hist(
        self, col_name: str, hist_bins: int | None = None
    ) -> DataFrame:
        if hist_bins is None:
            hist_bins = 8

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

    def get_quantile(self, col_name: str, quants: list[float]) -> DataFrame:
        return self.df.select(
            [
                nw.col(col_name)
                .quantile(q, interpolation="linear")
                .round(2)
                .alias(f"q{int(q*100)}")
                for q in quants
            ]
        )

    def interquartile_range(self, col_name: str) -> DataFrame:
        return self.get_quantile(col_name, [0.25, 0.75]).select(
            (nw.col("q75") - nw.col("q25")).round(2).alias("iqr")
        )

    def maybe_outliers(self, col_name: str, total_only: bool | None = None) -> DataFrame:
        lower_fence = self.get_quantile(col_name, [0.25]).item(
            0, 0
        ) - self.interquartile_range(col_name).item(0, 0)
        upper_fence = self.get_quantile(col_name, [0.75]).item(
            0, 0
        ) + self.interquartile_range(col_name).item(0, 0)

        df_filt = self.df.clone()
        df_filt = df_filt.select(
            lower_count=nw.col(col_name).filter(nw.col(col_name) < lower_fence).count(),
            upper_count=nw.col(col_name).filter(nw.col(col_name) > upper_fence).count(),
        )
        if total_only:
            return df_filt.select(
                nw.sum_horizontal("lower_count", "upper_count").alias("outliers")
            )
        else:
            return df_filt.with_columns(
                nw.sum_horizontal("lower_count", "upper_count").alias("outliers")
            )

    def concat_horizontal(self, col_name: str) -> DataFrame:
        col = nw.col(col_name)
        return nw.concat(
            [
                self.df.select(col.null_count().alias("Nulls")),
                self.df.select(
                    (col.null_count() * 100 / nw.lit(self.df.shape[0])).alias("Null%")
                ),
                self.df.select(mean=col.mean().round(2)),
                self.df.select(std=col.std().round(2)),
                self.df.select(min=col.min().round(2)),
                self.get_quantile(col_name, quants=[0.25]),
                self.df.select(median=col.median().round(2)),
                self.get_quantile(col_name, quants=[0.75]),
                self.df.select(max=col.max().round(2)),
                self.maybe_outliers(col_name, total_only=True),
                self.value_frequency_hist(col_name),
            ],
            how="horizontal",
        )

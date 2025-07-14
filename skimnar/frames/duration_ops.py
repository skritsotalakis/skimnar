from __future__ import annotations

from enum import Enum
import narwhals as nw
from skimnar.base_ops import BaseFrame
from narwhals.dataframe import DataFrame
from typing import Union


class TimeUnit(Enum):
    YEARS = 86400 * 365
    DAYS = 86400
    HOURS = 3600
    SECONDS = 1
    NANOSECONDS = 1 / 10**9

    @classmethod
    def from_string(cls, unit: str) -> TimeUnit:
        for member in cls:
            if unit.casefold() == member.name.casefold():
                return member
        raise TypeError(f"Unsupported unit: {unit}")


class DurationFrame(BaseFrame):
    def __init__(self, df: DataFrame, *, time_unit: Union[TimeUnit, str]):
        if isinstance(time_unit, str):
            self.time_unit = TimeUnit.from_string(time_unit)
        else:
            self.time_unit = time_unit

        super().__init__(df, "duration")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(dtype={self.frame_type}, columns={self.df.columns}, shape={self.df.shape}, (<TimeUnit.{self.time_unit.name}: {self.time_unit.value} seconds>))"

    def _conversion(self, col_name: str) -> DataFrame:
        unit_multiplier = self.time_unit.value * 10**9
        return nw.maybe_convert_dtypes(
            self.df.select(nw.col(col_name).dt.total_nanoseconds() / unit_multiplier)
        )

    def concat_horizontal(self, col_name: str) -> DataFrame:
        col = nw.col(col_name)
        return nw.concat(
            [
                nw.from_dict(
                    {"unit": [self.time_unit.name]}, backend=self.df.implementation
                ),
                self._conversion(col_name).select(col.null_count().alias("Nulls")),
                self._conversion(col_name).select(
                    (col.null_count() * 100 / nw.lit(self.df.shape[0])).alias("Null%")
                ),
                self._conversion(col_name).select(
                    col.cast(nw.Float64).min().round(2).alias("min")
                ),
                self._conversion(col_name).select(
                    col.cast(nw.Float64).mean().round(2).alias("mean")
                ),
                self._conversion(col_name).select(
                    col.cast(nw.Float64).max().round(2).alias("max")
                ),
            ],
            how="horizontal",
        )

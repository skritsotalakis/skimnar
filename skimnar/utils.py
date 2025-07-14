from __future__ import annotations

from narwhals.dataframe import DataFrame
from narwhals.typing import IntoDataFrame
import narwhals as nw
import narwhals.selectors as ncs
import narwhals.dtypes as nw_dtypes

from typing import TYPE_CHECKING, Dict, Type, Union, Any

if TYPE_CHECKING:
    from skimnar.base_ops import BaseFrame


Dtype_nw = Union[
    Type[nw_dtypes.NumericType],
    Type[nw_dtypes.String],
    Type[nw_dtypes.Boolean],
    Type[nw_dtypes.Categorical],
    Type[nw_dtypes.Datetime],
    Type[nw_dtypes.Duration],
]


def _selector_map() -> Dict[str, Any]:
    return {
        "numeric": ncs.numeric(),
        "string": ncs.string(),
        "boolean": ncs.boolean(),
        "categorical": ncs.categorical(),
        "datetime": ncs.datetime(),
        "duration": ncs.by_dtype(nw.Duration),
    }


def dtype_instance_map() -> Dict[Type[BaseFrame], Dtype_nw]:
    from skimnar.frames import (
        NumericFrame,
        StringFrame,
        BooleanFrame,
        CategoricalFrame,
        DatetimeFrame,
        DurationFrame,
    )

    return {
        NumericFrame: nw_dtypes.NumericType,
        StringFrame: nw_dtypes.String,
        BooleanFrame: nw_dtypes.Boolean,
        CategoricalFrame: nw_dtypes.Categorical,
        DatetimeFrame: nw_dtypes.Datetime,
        DurationFrame: nw_dtypes.Duration,
    }


def get_frame_type(dtype: Dtype_nw) -> Any:
    for frame_type, nw_dtype_class in dtype_instance_map().items():
        if isinstance(dtype, nw_dtype_class):
            return frame_type
    return None


def from_valid_native(df_native: IntoDataFrame) -> DataFrame:
    try:
        df = nw.from_native(df_native, eager_only=True)
    except TypeError:
        error_msg = """Expected a Narwhals-native Eager DataFrame or series (https://narwhals-dev.github.io/narwhals/).
                    Got {type(df_native).__module__} instead."""
        raise TypeError(error_msg)

    if df.is_empty():
        raise ValueError("Provided dataframe is empty!")
    return df

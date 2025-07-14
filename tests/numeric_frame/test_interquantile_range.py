from __future__ import annotations

import pytest

import pandas as pd
import polars as pl
import pyarrow as pa
from tests.nw_utils import ConstructorEager, assert_equal_data
from skimnar.utils import from_valid_native
from skimnar.frames.numeric_ops import NumericFrame


@pytest.mark.parametrize("constructor", [pd.DataFrame, pl.DataFrame, pa.table])  # type: ignore[misc]
def test_interquartile_range(constructor: ConstructorEager) -> None:
    data = {"a": [1, 3, 2, 4, 4, 6, 7, 8, 9, 0, 1, 1, 6, 7, 7]}
    df = from_valid_native(constructor(data))
    results = NumericFrame(df).interquartile_range("a")
    expected = {"iqr": [5.5]}
    assert_equal_data(results, expected)

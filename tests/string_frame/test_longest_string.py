from __future__ import annotations

import pytest

import pandas as pd
import polars as pl
import pyarrow as pa
from tests.nw_utils import ConstructorEager, assert_equal_data
from skimnar.utils import from_valid_native
from skimnar.frames.string_ops import StringFrame


@pytest.mark.parametrize("constructor", [pd.DataFrame, pl.DataFrame, pa.table])  # type: ignore[misc]
def test_longest_string(constructor: ConstructorEager) -> None:
    data = {"a": ["cat", "doors", "socks", "air purifier", "debt", "stink"]}
    df = from_valid_native(constructor(data))
    results = StringFrame(df).longest_string("a")
    expected = {"longest_string": ["air purifier"]}
    assert_equal_data(results, expected)

# Skimnar

Skimnar is a tool that provides summary statistics in the terminal for variables of different data types in dataframes.

Additionally, users can get data type specific summary dataframes and tables.

It is inspired from the skimpy tool and utilizes narwhals and rich libraries.


## Usage

### A. Get summary statistics for all columns of your dataframe:

Given a narwhals or a narwhals-native dataframe (e.g. pandas, Polars, Pyarrow), skimnar produces summary statistics in the terminal.

```python
from skimnar import skimnar
skimnar(df_native)
```
### B. Get data type specific objects:

Import any data type specific skimnar class object (NumericFrame, StringFrame, CategoricalFrame, BooleanFrame, DatetimeFrame, DurationFrame) and use it to get back summary statistics or rich tables for any of those classes.

#### I. Get back narwhals dataframe containing summary statistics
Use the output to apply extra operations leveraging narwhals api or just get back a dataframe of your choice.

Example:

```python
import pandas as pd
df_native = pd.read_parquet("tests/data/synthetic_dataset.parquet")
from skimnar.frames.numeric_ops import NumericFrame
df = NumericFrame(df_native).sum_df # df is a narwhals dataframe
df.to_pandas() # Get back a pandas dataframe
```

```
   Nulls  Null%   mean    std   min   q25  median    q75    max  outliers      hist
0     50    5.0   0.51   0.30 -0.47  0.31    0.51   0.70   1.66        43   ▁▄▇▅▂
0      0    0.0   2.07   1.00 -0.94  1.39    2.06   2.73   5.19        47   ▁▄▇▆▄▁
0      0    0.0  10.02   2.95  0.94  8.06   10.00  11.98  21.78        47   ▂▅▇▅▂
0     30    3.0   1.93   1.95  0.00  0.57    1.32   2.61  12.19        87  ▇▃▂▁
0      0    0.0  14.94  14.49 -9.95  2.58   14.27  27.43  39.97         0  ▆▇▇▆▆▆▆▇
0      0    0.0   3.96   2.85  0.09  1.95    3.24   5.38  19.26        63  ▇▇▃▁▁
```

Example:

```python
import pandas as pd
df_native = pd.read_parquet("tests/data/synthetic_dataset.parquet")
from skimnar.frames.numeric_ops import NumericFrame
num_frame = NumericFrame()
num_frame.get_sum_df("weight")
```

```

┌───────────────────────────────────────────────────────────────────────────────┐
|                              Narwhals DataFrame                               |
|-------------------------------------------------------------------------------|
|   Nulls  Null%  mean   std  min   q25  median   q75    max  outliers      hist|
|0     30    3.0  1.93  1.95  0.0  0.57    1.32  2.61  12.19        87  ▇▃▂▁    |
└───────────────────────────────────────────────────────────────────────────────┘
```

#### II. Get back a rich table of summary statistics
```python
import polars as pl
df_native = pl.DataFrame({...})
from skimnar.frames.string_ops import StringFrame
StringFrame(df_native).table
```

```
STRING
┏━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━┓
┃ Column Name  ┃ Nulls ┃ Null% ┃ total_chars ┃ chars/row ┃ total_words ┃ words/row ┃
┡━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━┩
│ product_name │   0   │  0.0  │    12000    │   12.0    │    2000     │    2.0    │
│ mixed_object │   0   │  0.0  │    11568    │   11.57   │    2669     │   2.67    │
└──────────────┴───────┴───────┴─────────────┴───────────┴─────────────┴───────────┘
```



## Installation

```bash
git clone https://github.com/skritsotalakis/skimnar.git
cd skimnar
pip install -e .
```


## Features

- Rich display of dataframe summaries using narwhals
- Data type specific objects operations
- Support for narwhals eager native frames
- Support for different data types (numeric, categorical, boolean, string, datetime, duration)
- Extensible architecture
- Typed


## License

Distributed under the terms of the [MIT](https://opensource.org/license/MIT) license, skimnar is free and open source software.

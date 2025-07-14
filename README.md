# Skimnar

Skimnar is a tool that provides summary statistics in the terminal for variables of different data types in dataframes.
Additionally, users can get data type specific summary dataframes and tables.
It is inspired from the skimpy tool and utilizes narwhals and rich libraries.


## Usage

### A. Get summary statistics for all columns of your dataframe:

Given a narwhals-native dataframe (pandas, Polars, Pyarrow), skimnar produces summary statistics in the terminal.

```python
from skimnar import skimnar
skimnar(df_native)
```
### B. Get data type specific objects:

Import any data type specific skimnar class objcet (NumericFrame, StringFrame, CategoricalFrame, BooleanFrame, DatetimeFrame, DurationFrame) and use it to get back summary statistics or rich tables for any of those classes.

#### I. Get back narwhals dataframe containing summary statistics
Use the output to apply extra operations leveraging narwhals api or just get back a dataframe of your choice.


```python
import pandas as pd
df_native = pd.DataFrame({...})
from skimnar.frames.numeric_ops import NumericFrame
df = NumericFrame(df_native).sum_df # df is a narwhals dataframe
df.to_pandas() # Get back a pandas dataframe
```
#### II. Get back a rich table of summary statistics
```python
import polars as pl
df_native = pl.DataFrame({...})
from skimnar.frames.string_ops import StringFrame
StringFrame(df_native).table
```



## Installation

```bash
git clone git@github.com:skritsotalakis/skimnar.git
cd skimnar
pip install -e .
```


## Features

- Rich display of dataframe summaries using narwhals
- Data type specific objects operations
- Support for narwhals eager native frames and serries
- Support for different data types (numeric, categorical, boolean, string, datetime, duration)
- Extensible architecture
- Typed


## License

Distributed under the terms of the [MIT](https://opensource.org/license/MIT) license, skimpy is free and open source software.

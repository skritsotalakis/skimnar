from abc import ABC, abstractmethod
from skimnar.utils import _selector_map, from_valid_native
import narwhals as nw
import rich
from rich.table import Table
from typing import Union, Literal, List
from narwhals.typing import IntoDataFrame
from narwhals.dataframe import DataFrame


class BaseFrame(ABC):
    def __init__(self, df: Union[IntoDataFrame, DataFrame], frame_type: str):
        if isinstance(df, DataFrame):
            self.base_df = df
        else:
            self.base_df = from_valid_native(df)
        if frame_type not in _selector_map():
            raise KeyError(
                f"Unknown data type: {frame_type}.\n Accepted data types are {list(_selector_map().keys())}"
            )
        self._dtypes = list(self.base_df.schema.values())
        self.frame_type = frame_type
        self.selector = _selector_map()[self.frame_type]
        self.df = self.select_frame()
        self.sum_df = self.get_sum_df(self.df.columns)
        self.table = self.frame_to_table(self.frame_type.upper())

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(dtype={self.frame_type}, columns={self.df.columns}, shape={self.df.shape})"

    @abstractmethod
    def concat_horizontal(self, col_name: str) -> DataFrame:
        pass

    def is_column_null(self, col_name: str) -> Literal[True, False]:
        if self.df[col_name].null_count() == self.df[col_name].len():
            return True
        return False

    def select_frame(self) -> DataFrame:
        frame = self.base_df.select(self.selector)
        if frame.is_empty():
            raise ValueError(f"The dataframe has no {self.frame_type} columns.")
        return frame

    def get_sum_df(self, col_list: Union[str, List[str]]) -> DataFrame:
        """Returns the summary DataFrame for all columns."""
        if isinstance(col_list, str):
            col_list = [col_list]

        results = []
        for col_name in col_list:
            if not self.is_column_null(col_name):
                result = self.concat_horizontal(col_name)
                results.append(result)
        return nw.concat(results, how="vertical")

    def get_base_df_info(self) -> List[rich.table.Table]:
        shape_dict = {
            "Number of rows": self.base_df.shape[0],
            "Number of columns": self.base_df.shape[1],
        }
        table1 = Table(
            show_header=True,
            header_style="magenta",
            title="OVERVIEW",
            title_justify="left",
        )
        table1.add_column("Structure", justify="left")
        table1.add_column("Shape", justify="center")
        for key, val in shape_dict.items():
            table1.add_row(key, str(val))
        types_dict = {
            str(_dtype): self._dtypes.count(_dtype)
            for _dtype in self.base_df.schema.dtypes()
        }
        table2 = Table(
            show_header=True, header_style="magenta", title=" ", title_justify="left"
        )
        table2.add_column("Data Type", justify="left")
        table2.add_column("Count", justify="center")
        for key, val in types_dict.items():
            table2.add_row(key, str(val))
        return [table1, table2]

    def frame_to_table(self, title: str) -> rich.table.Table:
        """Returns a rich table from a dataframe."""
        df = self.sum_df.select(nw.all().cast(nw.String))
        table = Table(
            show_header=True, header_style="magenta", title=title, title_justify="left"
        )
        table.add_column("Column Name", justify="left")
        for col in df.columns:
            table.add_column(col, justify="center")

        row = []
        for size in range(df.shape[0]):
            row = [self.df.columns[size]]
            row += [df[col].cast(nw.String)[size] for col in df.columns]
            table.add_row(*row)
        return table

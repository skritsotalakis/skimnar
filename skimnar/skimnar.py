import subprocess
from io import StringIO
from rich.console import Console
from rich.columns import Columns
from rich.panel import Panel
from rich.table import Table
from typing import Any
from skimnar.frames import DurationFrame
from skimnar.utils import from_valid_native, get_frame_type
from narwhals.typing import IntoDataFrame


def skimnar(df_native: IntoDataFrame) -> Any:
    df = from_valid_native(df_native)

    _types = list(
        dict.fromkeys(
            get_frame_type(dtype)
            for dtype in df.schema.dtypes()
            if get_frame_type(dtype) is not None
        )
    )

    console = Console(file=StringIO(), record=True, force_terminal=True)

    grid = Table.grid(expand=True, padding=(0, 0))

    if not _types:
        grid = "We regret to inform you that we cannot proccess any of the columns from the provided dataframe."
    else:
        intro_panel = _types[0](df).get_base_df_info()
        grid.add_row(Columns(intro_panel))
        for _type in _types:
            if _type is DurationFrame:
                panel_table = _type(df, time_unit="DAYS").table
            else:
                panel_table = _type(df).table

            type_specific_panel = Panel(
                panel_table, title="", border_style="black", padding=(0, 0)
            )
            grid.add_row(type_specific_panel)

    final_panel = Panel(
        grid,
        title="SUMMARY START",
        subtitle="SUMMARY END",
        border_style="bold  #A9A9A9",
        padding=(1, 0),
    )

    console.print(final_panel)

    output = console.export_text(styles=True)
    subprocess.run(["less", "-R"], input=output, text=True)

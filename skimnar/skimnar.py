import subprocess
from io import StringIO
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from typing import Any
from skimnar.frames import DurationFrame
from skimnar.utils import from_valid_native, get_frame_type
from narwhals.typing import IntoDataFrame


def skimnar(df_native: IntoDataFrame) -> Any:
    df = from_valid_native(df_native)

    _types = [get_frame_type(dtype) for dtype in df.schema.dtypes()]
    _types = list(dict.fromkeys(_types))

    console = Console(file=StringIO(), record=True, force_terminal=True)

    grid = Table.grid(expand=True, padding=(0, 0))

    for _type in _types:
        if _type is None:
            continue
        if _type is DurationFrame:
            panel_table = _type(df, time_unit="DAYS").table
        else:
            panel_table = _type(df).table

        init_panel = Panel(panel_table, title="", border_style="black", padding=(0, 0))
        grid.add_row(init_panel)

    final_panel = Panel(
        grid,
        title="NARWOW SUMMARY START",
        subtitle="NARWOW SUMMARY END",
        border_style="bold  #A9A9A9",
        padding=(1, 0),
    )

    console.print(final_panel)

    output = console.export_text(styles=True)
    subprocess.run(["less", "-R"], input=output, text=True)

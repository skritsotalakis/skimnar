import narwhals as nw
from skimnar.base_ops import BaseFrame
from narwhals.dataframe import DataFrame
from typing import Optional, Dict


class StringFrame(BaseFrame):
    def __init__(self, df: DataFrame):
        super().__init__(df, "string")

    def shortest_string(self, col_name: str) -> DataFrame:
        argmin = self.df.select(nw.col(col_name).str.len_chars().arg_min())
        new_dict = {"shortest_string": [self.df.item(argmin.item(0, 0), 0)]}
        return nw.from_dict(new_dict, backend=self.df.implementation)

    def longest_string(self, col_name: str) -> DataFrame:
        argmax = self.df.select(nw.col(col_name).str.len_chars().arg_max())
        new_dict = {"longest_string": [self.df.item(argmax.item(0, 0), 0)]}
        return nw.from_dict(new_dict, backend=self.df.implementation)

    def alphabetic_min(self, col_name: str) -> DataFrame:
        new_dict = {
            "alphabetic_min": [self.df[col_name].drop_nulls().sort(descending=False)[0]]
        }
        return nw.from_dict(new_dict, backend=self.df.implementation)

    def alphabetic_max(self, col_name: str) -> DataFrame:
        new_dict = {
            "alphabetic_max": [self.df[col_name].drop_nulls().sort(descending=True)[0]]
        }
        return nw.from_dict(new_dict, backend=self.df.implementation)

    def replace_many(self, col_name: str, patterns: dict[str, str]) -> DataFrame:
        df_replaced = self.df.clone()
        for old, new in patterns.items():
            df_replaced = df_replaced.select(
                nw.col(col_name).str.replace_all(old, new, literal=True)
            )
        return df_replaced

    def total_words(
        self, col_name: str, replace_chars: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        if replace_chars is None:
            replace_chars = {
                "?": " ",
                "!": " ",
                ".": " ",
                ";": " ",
                "_": " ",
                ":": " ",
                ",": " ",
                "-": " ",
                "*": " ",
                "#": " ",
            }
        df_replaced = self.replace_many(col_name, replace_chars)
        new_list = df_replaced[col_name].drop_nulls().to_list()
        new_dict = {"total_words": [sum(len(phrase.split()) for phrase in new_list)]}
        return nw.from_dict(new_dict, backend=self.df.implementation)

    def words_per_row(self, col_name: str) -> DataFrame:
        return self.total_words(col_name).select(
            (nw.col("total_words") / nw.lit(self.df.shape[0])).round(2).alias("words/row")
        )

    def total_chars(self, col_name: str) -> DataFrame:
        return self.df.select(
            nw.col(col_name)
            .str.replace_all(",", "")
            .str.replace_all(" ", "")
            .str.strip_chars('?!.";-:,')
            .str.len_chars()
            .sum()
            .alias("total_chars")
        )

    def chars_per_row(self, col_name: str) -> DataFrame:
        return self.total_chars(col_name).select(
            (nw.col("total_chars") / nw.lit(self.df.shape[0])).round(2).alias("chars/row")
        )

    def concat_horizontal(self, col_name: str) -> DataFrame:
        col = nw.col(col_name)
        return nw.concat(
            [
                self.df.select(col.null_count().alias("Nulls")),
                self.df.select(
                    (col.null_count() * 100 / nw.lit(self.df.shape[0])).alias("Null%")
                ),
                self.total_chars(col_name),
                self.chars_per_row(col_name),
                self.total_words(col_name),
                self.words_per_row(col_name),
            ],
            how="horizontal",
        )

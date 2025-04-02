import polars  as pl 
from etl import detect_encoding,detect_separator
import datetime 

file = 'data/accidents/datatran2019.csv'

encoding = detect_encoding(file)
separator = detect_separator(file, encoding)

df = pl.read_csv(
    file,
    separator=separator,
    has_header=True,
    infer_schema_length=0,
    truncate_ragged_lines=True,
    encoding=encoding,
    decimal_comma=False,
    ignore_errors=True,
)

df = df.unique(subset=["id"])

df = df.with_columns(
    pl.when(
        (pl.col("data_inversa").str.len_chars() == 10) &
        pl.col("data_inversa").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).is_not_null()
    ).then(
        pl.col("data_inversa").str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
    ).when(
        pl.col("data_inversa").str.strptime(pl.Date, format="%d/%m/%y", strict=False).is_not_null()
    ).then(
        pl.col("data_inversa").str.strptime(pl.Date, format="%d/%m/%y", strict=False)
    ).otherwise(
        pl.col("data_inversa").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
    ).alias("data_inversa")
)
print(df.height)



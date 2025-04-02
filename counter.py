import polars as pl 

file = 'all_accidents.csv'

df = pl.read_csv(file,separator=';', ignore_errors=True)

df = df.with_columns(
    pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
)


df = df.with_columns(pl.col("data").dt.year().alias("ano"))
contagem_por_ano = df.group_by("ano").agg(pl.len().alias("total_acidentes"))
print(contagem_por_ano)

import polars as pl 

file = 'all_tickets.csv'

df = pl.read_csv(file,separator=';', ignore_errors=True)

print(df.height)

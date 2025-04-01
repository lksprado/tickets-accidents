import polars as pl 


def fix_dates(df, col_name="data"):
    """Corrige a coluna de datas garantindo que anos abreviados sejam tratados corretamente."""
    df = df.with_columns(
        pl.when(
            pl.col(col_name).str.strptime(pl.Date, format="%d/%m/%Y", strict=False).is_not_null()
        ).then(
            pl.col(col_name).str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
        ).otherwise(
            pl.col(col_name).str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        ).alias(col_name)
    )
    return df 


def create_kpi(accident_file, tickets_file):
    
    df_a = pl.scan_csv(accident_file,separator=';', infer_schema_length=0)
    
    df_t = pl.scan_csv(tickets_file, separator=';', infer_schema_length=0)
    
    df_a = fix_dates(df_a, "data")   
    
    df_t = fix_dates(df_t, "data")   
    
    df_a = (df_a.unique(subset=["id"])
            .group_by("data")
            .agg(pl.len().alias("total_acidentes"))
            )
    df_t = df_t.group_by("data").agg(pl.len().alias("total_multas"))
    
    
    df = df_a.join(df_t, left_on="data", right_on="data", how="inner").sort("data")
    
    df.sink_csv('tickets_x_accidents.csv', separator=';')
    
if __name__ == '__main__':
    a_file = 'all_accidents.csv'
    t_file = 'all_tickets.csv'
    
    
    create_kpi(a_file,t_file)
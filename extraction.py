import os
import polars as pl

def consolidate_files_in_folder(folder_path: str) -> pl.DataFrame:
    path = folder_path
    folder_list = os.listdir(path)
    
    data = []
    all_columns = set()

    # PRIMEIRA PASSAGEM PARA PEGAR AS COLUNAS
    for file in folder_list:
        filename = os.path.basename(file)
        filepath = os.path.join(path, filename)
        df = pl.read_csv(filepath, separator=';', encoding='latin1', infer_schema=False)
        all_columns.update(df.schema.keys())  # Adiciona todas as colunas únicas

    # ADICIONA COLUNA COM NULO SENAO EXISTIRNO SET E ORDENA
    for file in folder_list:
        filename = os.path.basename(file)
        filepath = os.path.join(path, filename)
        df = pl.read_csv(filepath, separator=';', encoding='latin1', infer_schema=False)
        
        missing_cols = all_columns - set(df.schema.keys())
        for col in missing_cols:
            df = df.with_columns(pl.lit(None).alias(col))
        
        df = df.select(sorted(all_columns))
        df = df.unique(subset=["id"])
        data.append(df)
    
    data_df = pl.concat(data, how='vertical_relaxed')
    
    return data_df 



if __name__ == '__main__':
    folder = '/media/lucas/Files/2.Projetos/accidents-tickets/data/accidents/'
    df = consolidate_files_in_folder(folder)
    # df.write_csv('all_accidents.csv', separator=';')

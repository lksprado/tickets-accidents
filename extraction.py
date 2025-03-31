import os
import polars as pl

import os
import polars as pl

import os
import polars as pl

import os
import polars as pl

def consolidate_files_in_folder(folder_path: str, output_file: str):
    """Lê e processa arquivos CSV na pasta sem sobrecarregar a RAM, tratando tudo como string."""
    
    all_columns = set()
    first_write = True

    # Remove o arquivo de saída se já existir
    if os.path.exists(output_file):
        os.remove(output_file)

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)
                print(f"Processando: {file}")
                
                try:
                    df = pl.read_csv(
                        filepath, 
                        separator=';', 
                        encoding='latin1', 
                        infer_schema_length=0  # Desativar inferência de tipos
                    )
                except:
                    df = pl.read_csv(
                        filepath, 
                        separator=',', 
                        encoding='latin1', 
                        infer_schema_length=0
                    )
                
                # Converter todas as colunas para string
                df = df.with_columns([pl.col(col).cast(pl.Utf8) for col in df.columns])
                df = df.rename({col: col.lower() for col in df.columns})
                all_columns.update(df.columns)
                
                missing_cols = all_columns - set(df.columns)
                for col in missing_cols:
                    df = df.with_columns(pl.lit("None").cast(pl.Utf8).alias(col))
                
                df = df.select(sorted(all_columns))
                
                # Escrever com delimitador ponto e vírgula
                if first_write:
                    df.write_csv(output_file, separator=';')  # Escreve com cabeçalho
                else:
                    with open(output_file, 'a', encoding='latin1') as f:
                        df.write_csv(f, separator=';', include_header=False)  # Anexa sem cabeçalho
                first_write = False

    print("Arquivo consolidado salvo em:", output_file)




if __name__ == '__main__':
    folder_tickets = '/media/lucas/Files/2.Projetos/accidents-tickets/data/tickets/'
    consolidate_files_in_folder(folder_tickets, 'all_tickets.csv')


def transform_accidents_df(df_input: pl.DataFrame, cols_list: list)-> pl.DataFrame:
    df = df_input
    cols = cols_list 
    
    df = df.drop(cols)
    df = df.unique(subset=["id"])
    
    df = df.with_columns(
        pl.when(
            pl.col("data_inversa").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).is_not_null()
        ).then(
            pl.col("data_inversa").str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
        ).otherwise(
            pl.col("data_inversa").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        ).alias("data_inversa")
    )
    return df 

def transform_tickets_df(df_input: pl.DataFrame, cols_list: list)-> pl.DataFrame:
    df = df_input
    cols = cols_list 
    
    df = df.drop(cols)
    df = df.unique(subset=["id"])
    
    df = df.with_columns(
        pl.when(
            pl.col("dat_infracao").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).is_not_null()
        ).then(
            pl.col("dat_infracao").str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
        ).otherwise(
            pl.col("dat_infracao").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        ).alias("dat_infracao")
    )
    return df 



if __name__ == '__main__':
    folder = '/media/lucas/Files/2.Projetos/accidents-tickets/data/accidents/'
    cols_accident = [
            'br',
            'delegacia',
            'ignorados',
            'km',
            'municipio',
            'regional',
            'uop'
        ]
    
    cols_tickets = [
        'data_fim_vigencia',
        'data_inicio_vigencia',
        'enquadramento',
        'exc_verificado',
        'ind_assinou_auto',
        'ind_sentido_trafego',
        'ind_veiculo_estrangeiro',
        'med_considerada',
        'med_realizada',
        'nom_modelo_veiculo',
        'tip_abordagem',
        'uf_placa'
    ]


    folder_tickets = '/media/lucas/Files/2.Projetos/accidents-tickets/data/tickets/'
    
    # df = consolidate_files_in_folder(folder)
    # df = transform_accidents_df(df,cols_accident)
    # df.write_csv('all_accidents.csv', separator=';')
    
    df2 = consolidate_files_in_folder(folder_tickets, 'all_tickets.csv')




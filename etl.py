import os
from unidecode import unidecode
import chardet
import polars as pl 
import os 

def sanitize_column_names(df, cols_to_remove:list, cols_to_rename:dict):
    """Sanitiza os nomes das colunas."""
    df.columns = [unidecode(col) for col in df.columns]  # Remove acentos
    df.columns = [col.replace(' ', '_') for col in df.columns]
    df.columns = [col.replace('ç', 'c') for col in df.columns]  # Substitui 'ç' por 'c'
    df.columns = [col.replace(r'\W', '') for col in df.columns]  # Remove caracteres especiais
    df.columns = [col.lower() for col in df.columns]  # Converte para minúsculas
    df.columns = [col.replace('"', '') for col in df.columns]  # Remove aspas
    
    df = df.drop([col for col in cols_to_remove if col in df.columns], strict=False)
    
    rename_dict = {old: new for old, new in cols_to_rename.items() if old in df.columns}
    df = df.rename(rename_dict)
    return df

def detect_encoding(filepath):
    """Detecta a codificação do arquivo usando o chardet."""
    with open(filepath, 'rb') as f:
        raw_data = f.read(10000)  # Lê uma amostra de 10k bytes
    result = chardet.detect(raw_data)
    return result['encoding']

def detect_separator(filepath, encoding):
    """Detecta automaticamente o separador correto do CSV analisando a primeira linha."""
    with open(filepath, 'r', encoding=encoding, errors='replace') as f:
        first_line = f.readline()
    # Conta quantos separadores existem e escolhe o mais frequente
    semicolon_count = first_line.count(';')
    comma_count = first_line.count(',')
    
    return ';' if semicolon_count > comma_count else ','

def process_files(file: str, cols_to_remove:list, cols_to_rename:dict):
    encoding = detect_encoding(file)
    separator = detect_separator(file, encoding)
    try:
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
    except Exception as e :
        print(f"Error {file} --- {e}")
    
    df = sanitize_column_names(df, cols_to_remove, cols_to_rename)

    print("Processed:", file)
    return df 

def transform_accidents_df(df_input: pl.DataFrame) -> pl.DataFrame:
    df = df_input.unique(subset=["id"])

    df = df.with_columns(
        pl.when(
            (pl.col("data").str.len_chars() == 10) &
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).is_not_null()
        ).then(
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
        ).when(
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%y", strict=False).is_not_null()
        ).then(
            # Tratando anos de dois dígitos para expandi-los corretamente
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%y", strict=False)
        ).otherwise(
            pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        ).alias("data")
    )

    return df

def transform_tickets_df(df_input: pl.DataFrame)-> pl.DataFrame:
    df = df_input  
    
    df = df.with_columns(
        pl.when(
            (pl.col("data").str.len_chars() == 10) &
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).is_not_null()
        ).then(
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%Y", strict=False)
        ).when(
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%y", strict=False).is_not_null()
        ).then(
            # Tratando anos de dois dígitos para expandi-los corretamente
            pl.col("data").str.strptime(pl.Date, format="%d/%m/%y", strict=False)
        ).otherwise(
            pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
        ).alias("data")
    )
    
    # Agrupar pelos campos especificados e contar ocorrências
    df_grouped = (
        df.group_by(["data", "uf_infracao", "municipio", "descricao"])
        .agg(pl.len().alias("total_multas"))
        .sort("total_multas", descending=True)  # Ordena pelo maior número de ocorrências
    )

    return df_grouped

def run_tickets(input_folder:str, output_file:str):
    remove = [
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
        'uf_placa',
        'cod_infracao',
        'hora',
        'indicador_de_abordagem',
        'assinatura_do_auto',
        'sentido_trafego',
        'indicador_veiculo_estrangeiro',
        'descricao_especie_veiculo',
        'descricao_tipo_veiculo',
        'descricao_modelo_veiculo',
        'codigo_da_infracao',
        'enquadramento_da_infracao',
        'inicio_vigencia_da_infracao',       
        'fim_vigencia_infracao',
        'medicao_infracao',
        'hora_infracao',
        'medicao_considerada'
        'excesso_verificado',
        'qtd_infracoes',
        'inicio_vigencia_da_infracao',
        'numero_do_auto',
        'marca',
        'especie'
    ]

    rename = { 
        'data_da_infracao_(dd/mm/aaaa)':'data',
        'nom_municipio':'municipio',
        'dat_infracao':'data',
        'num_br_infracao':'br',
        'nome_veiculo_marca':'marca',
        'num_km_infracao': 'km_infracao',
        'br_infracao':'br',
        'descricao_marca_veiculo':'marca',
        'descricao_abreviada_infracao':'descricao',
        'descricao_abreviada':'descricao'
        }
    first_write = True

    # Remove o arquivo de saída se já existir
    if os.path.exists(output_file):
        os.remove(output_file)

    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)
                df = process_files(filepath, cols_to_remove=remove, cols_to_rename=rename)
                df = transform_tickets_df(df)
                # Escrever com delimitador ponto e vírgula
                if first_write:
                    df.write_csv(output_file, separator=';')
                else:
                    with open(output_file, 'a') as f:
                        df.write_csv(f, separator=';', include_header=False)
                first_write = False

def run_accidents(input_folder:str, output_file:str):
    remove = [
        'delegacia',
        'latitude',
        'longitude',
        'sentido_via',
        'ano',
        'uop',
        'regional',
    ]

    rename = { 
        'data_inversa':'data',
        }
    
    first_write = True

    # Remove o arquivo de saída se já existir
    if os.path.exists(output_file):
        os.remove(output_file)

    for root, _, files in os.walk(input_folder):
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)

                df = process_files(filepath, cols_to_remove=remove, cols_to_rename=rename)
                df = transform_accidents_df(df)
                
                # Escrever com delimitador ponto e vírgula
                if first_write:
                    df.write_csv(output_file, separator=';')
                else:
                    with open(output_file, 'a') as f:
                        df.write_csv(f, separator=';', include_header=False)
                
                first_write = False

def create_kpi(accident_file, tickets_file):
    
    df_a = pl.scan_csv(accident_file,separator=';', infer_schema_length=0) 

    df_a = df_a.with_columns(
        pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
    )
    
    df_t = pl.scan_csv(tickets_file, separator=';', infer_schema_length=0)    

    df_t = df_t.with_columns(
        pl.col("data").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
    )
    
    df_a = (df_a.group_by("data").agg(pl.len().alias("total_acidentes")))

    df_t = df_t.group_by("data").agg(pl.col('total_multas').sum().alias("total_multas"))    
    
    df = df_t.join(df_a, left_on="data", right_on="data", how="left").sort("data")    
    df.sink_csv('tickets_x_accidents.csv', separator=';')
    print("accidents_x_tickers created!")

if __name__ == '__main__':
    folder_accidents = '/media/lucas/Files/2.Projetos/accidents-tickets/data/accidents/'    
    folder_tickets = '/media/lucas/Files/2.Projetos/accidents-tickets/data/tickets/'
    
    accident_file = 'all_accidents.csv'
    tickets_file = 'all_tickets.csv'
    
    # run_accidents(folder_accidents, accident_file)    
    run_tickets(folder_tickets, tickets_file)
    
    create_kpi(accident_file, tickets_file)




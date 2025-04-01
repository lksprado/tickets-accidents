### GETS ALL COLUMNS FROM FILES INSIDE FOLDERS

import os
import polars as pl
from unidecode import unidecode
import chardet
import csv
from polars.exceptions import ComputeError

def sanitize_column_names(df):
    """Sanitiza os nomes das colunas."""
    df.columns = [unidecode(col) for col in df.columns]  # Remove acentos
    df.columns = [col.replace(' ', '_') for col in df.columns]
    df.columns = [col.replace('ç', 'c') for col in df.columns]  # Substitui 'ç' por 'c'
    df.columns = [col.replace(r'\W', '') for col in df.columns]  # Remove caracteres especiais
    df.columns = [col.lower() for col in df.columns]  # Converte para minúsculas
    df.columns = [col.replace('"', '') for col in df.columns]  # Remove aspas
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

def read_csv_fallback(filepath, encoding):
    """Lê apenas o cabeçalho do CSV como fallback."""
    separator = detect_separator(filepath, encoding)
    with open(filepath, 'r', encoding=encoding, errors='replace') as f:
        
        reader = csv.reader(f, delimiter=separator, doublequote=True, quotechar='"')
        headers = next(reader) 
    return pl.DataFrame(schema=[unidecode(h).replace(' ', '_').lower() for h in headers])

def consolidate_columns_in_folder(folder_path: str, output_file: str):
    """Percorre todos os arquivos CSV na pasta e coleta o nome de todas as colunas, salvando em um CSV."""
    all_columns = set()
    counter = 0
    file_counter = 0
    for root, _, files in os.walk(folder_path):
        file_counter += len(files)
        for file in files:
            if file.endswith(".csv"):
                filepath = os.path.join(root, file)

                encoding = detect_encoding(filepath)
                separator = detect_separator(filepath, encoding)
                
                try:
                    df = pl.read_csv(
                        filepath,
                        separator=separator,
                        has_header=True,
                        infer_schema_length=0,
                        truncate_ragged_lines=True,
                        encoding=encoding,
                        n_rows=0,
                        decimal_comma=False,
                        ignore_errors=True,
                        quote_char='"',
                    )
                except Exception as e:
                    try:
                        df = read_csv_fallback(filepath, encoding)
                    except Exception as e3:
                        print(f"Falha ao processar {file} com fallback: {e}")
                        continue

                df = sanitize_column_names(df)
                all_columns.update(df.columns)
                
                for col in df.columns:
                    if len(col) > 50:
                        print(f"Erro: Coluna '{col}' em {file} excede 50 caracteres ({len(col)} caracteres). Execução interrompida.")
                        exit(1) 

                
                counter +=1
                print(f'Processed: {file} --- {counter}/{file_counter}')
            

    df_columns = pl.DataFrame({'colunas': list(all_columns)})
    df_columns.write_csv(output_file, include_header=False)
    print(f"Arquivo com todas as colunas consolidado em: {output_file}")

# Exemplo de uso
if __name__ == '__main__':
    folder_tickets = '/media/lucas/Files/2.Projetos/accidents-tickets/data/accidents/'
    consolidate_columns_in_folder(folder_path=folder_tickets, output_file='colunas_accidents.csv')
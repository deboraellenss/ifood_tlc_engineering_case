from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from config import TAXI_TYPES
from functools import reduce
from datetime import datetime
import os
import re


def extract_date_reference(file_path):
    """
    Extract date reference from filename.
    
    Args:
        file_path: Path to the file
        
    Returns:
        datetime: Extracted date or default date (1900-01-01)
    """
    # Regex to extract year and month: assumes something like *_2023_03.parquet
    date_pattern = re.compile(r"(\d{4})[_-](\d{2})")
    match = date_pattern.search(file_path)
    
    if match:
        year, month = map(int, match.groups())
        try:
            return datetime(year, month, 1)
        except ValueError:
            print(f"Invalid date values in filename: {file_path}")
    
    print(f"Could not extract date from filename: {file_path}")
    return datetime(1900, 1, 1)  # Default date


def ler_e_padronizar_parquets(spark, pasta_parquet, de_para, colunas_finais=None):
    """
        Read and standardize Parquet files from a given directory.
        
        Args:
            spark (SparkSession): Active Spark session
            pasta_parquet (str): Path to directory containing Parquet files
            de_para (dict[str, list[str]]): Mapping of standardized column names to their variants
            colunas_finais (list, optional): List of final columns to keep. Defaults to None (keep all)
        
        Returns:
            DataFrame: A unified DataFrame with renamed and standardized columns, including metadata columns
        
        Raises:
            ValueError: If no compatible files are found in the specified directory
        """
    arquivos = [
        os.path.join(pasta_parquet, f)
        for f in os.listdir(pasta_parquet)
        if f.endswith(".parquet")
    ]
    dataframes = []
    colunas_finais = list(de_para.keys())

    for arquivo in arquivos:
        df = spark.read.parquet(arquivo)

        # Descobre as colunas presentes no arquivo que existem no de_para
        mapeamento_arquivo = {}
        for nome_padrao, variantes in de_para.items():
            for v in variantes:
                if v in df.columns:
                    mapeamento_arquivo[v] = nome_padrao
                    break  # Pega a primeira que encontrar

        # Detecta tipo com base no prefixo
        source_type = "desconhecido"
        for prefixo, tipo in TAXI_TYPES.items():
            if prefixo in arquivo:
                source_type = tipo
                break

        if not mapeamento_arquivo:
            continue  # pula se nenhum campo do de_para está no arquivo

        # Renomeia as colunas no DataFrame
        df_renomeado = (
            df.select(
                [
                    col(orig).alias(mapeamento_arquivo[orig])
                    for orig in mapeamento_arquivo
                ]
            )
            .withColumn("source_file", lit(arquivo))
            .withColumn("source_type", lit(source_type))
            .withColumn("ingestion_date", current_timestamp())
            .withColumn("date_ref", lit(extract_date_reference(arquivo)))
        )

        # Adiciona colunas ausentes com None
        for nome_padrao in colunas_finais:
            if nome_padrao not in df_renomeado.columns:
                df_renomeado = df_renomeado.withColumn(
                    nome_padrao, lit(None).cast("string")
                )

        dataframes.append(df_renomeado)

    if not dataframes:
        raise ValueError("Nenhum arquivo compatível encontrado na pasta.")

    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes
    )

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from functools import reduce
from datetime import datetime
import os
import re

def date_reference(file):
    # Regex to extract year and month: assumes something like *_2023_03.parquet
    date_pattern = re.compile(r"(\d{4})[_-](\d{2})")
    match = date_pattern.search(file)
    if match:
        year, month = map(int, match.groups())
        file_date = datetime(year, month, 1)
    else:
        file_date = datetime(1900, 1, 1)
    return file_date

def ler_e_padronizar_parquets(spark,pasta_parquet, de_para, colunas_finais=None):
    """
    - pasta_parquet: caminho para os arquivos Parquet
    - de_para: dict[str, list[str]] - nomes padronizados -> nomes equivalentes
    - colunas_finais: lista das colunas padronizadas que você quer manter (se None, mantém todas)

    Retorna um único DataFrame unificado com colunas renomeadas e padronizadas.
    """
    arquivos = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith(".parquet")]
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

        if not mapeamento_arquivo:
            continue  # pula se nenhum campo do de_para está no arquivo

        # Renomeia as colunas no DataFrame
        df_renomeado = df.select([col(orig).alias(mapeamento_arquivo[orig]) for orig in mapeamento_arquivo])\
                   .withColumn("ingestion_date", current_timestamp())\
                   .withColumn("date_ref", lit(date_reference(arquivo)))
        
        # Adiciona colunas ausentes com None
        for nome_padrao in colunas_finais:
            if nome_padrao not in df_renomeado.columns:
                df_renomeado = df_renomeado.withColumn(nome_padrao, lit(None).cast("string"))

        
        dataframes.append(df_renomeado)

    if not dataframes:
        raise ValueError("Nenhum arquivo compatível encontrado na pasta.")

    return reduce(
        lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
        dataframes
    )

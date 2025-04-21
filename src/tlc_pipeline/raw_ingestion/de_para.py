from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import os
import shutil

# Cria sessão Spark
spark = SparkSession.builder \
    .appName("Unificar Dados Parquet") \
    .getOrCreate()



import os
import re
from collections import defaultdict
from difflib import get_close_matches


import json
import csv

def move_to_quarantine(file_path, quarantine_path="./quarantine"):
    os.makedirs(quarantine_path, exist_ok=True)
    try:
        shutil.move(file_path, os.path.join(quarantine_path, os.path.basename(file_path)))
        print(f"🔁 Arquivo movido para quarentena: {file_path}")
    except Exception as e:
        print(f"Erro ao mover {file_path} para quarentena: {e}") 

def exportar_de_para_json(de_para_dict, caminho_arquivo):
    with open(caminho_arquivo, "w", encoding="utf-8") as f:
        json.dump(de_para_dict, f, indent=2, ensure_ascii=False)

def exportar_de_para_csv(de_para_dict, caminho_arquivo):
    with open(caminho_arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["nome_padrao", "colunas_equivalentes"])
        for nome_padrao, colunas in de_para_dict.items():
            writer.writerow([nome_padrao, ", ".join(colunas)])

def corrigir_de_para_colunas_exclusivas(de_para_dict, colunas_exclusivas):
    novo_de_para = {}

    for nome_padrao, colunas in de_para_dict.items():
        exclusivas_no_grupo = [c for c in colunas if c in colunas_exclusivas]
        restantes = [c for c in colunas if c not in colunas_exclusivas]

        # Se o grupo não tiver exclusivas, mantemos igual
        if not exclusivas_no_grupo:
            novo_de_para[nome_padrao] = colunas
        else:
            # Se tiver exclusivas e outros misturados, separamos
            if restantes:
                novo_de_para[nome_padrao] = restantes
            for exclusiva in exclusivas_no_grupo:
                novo_de_para[normalizar_coluna(exclusiva)] = [exclusiva]

    return novo_de_para


def normalizar_coluna(nome):
    return re.sub(r'[^a-zA-Z0-9]', '', nome.strip().lower())

def sugerir_de_para(spark, pasta_parquet, limite_similaridade=0.8, colunas_exclusivas=None):
    error_files = []
    if colunas_exclusivas is None:
        colunas_exclusivas = []

    arquivos = [os.path.join(pasta_parquet, f) for f in os.listdir(pasta_parquet) if f.endswith(".parquet")]
    colunas_encontradas = set()

    for arquivo in arquivos:
        try:
            df = spark.read.parquet(arquivo)
            colunas_encontradas.update(df.columns)
            
        except Exception as e:
            print(f"⚠️ Erro ao ler {arquivo}: {e}")
            error_files.append((arquivo, str(e)))
            move_to_quarantine(arquivo, quarantine_path="./quarantine")

    colunas_normalizadas = {col: normalizar_coluna(col) for col in colunas_encontradas}

    agrupamento = defaultdict(list)
    ja_agrupadas = set()

    for col, norm in colunas_normalizadas.items():
        if col in ja_agrupadas:
            continue

        # Se for coluna exclusiva, já agrupa sozinha
        if col in colunas_exclusivas:
            agrupamento[norm].append(col)
            ja_agrupadas.add(col)
            continue

        similares = get_close_matches(norm, colunas_normalizadas.values(), cutoff=limite_similaridade)
        grupo = [orig for orig, n in colunas_normalizadas.items() if n in similares]
        for g in grupo:
            ja_agrupadas.add(g)
        agrupamento[norm].extend(grupo)

    return agrupamento

def padronizar_de_para(sugestoes, nomes_padrao_manualmente_definidos=None):
    """
    - sugestoes: dict[str, list[str]] vindo do sugerir_de_para()
    - nomes_padrao_manualmente_definidos: dict[str, str] onde a chave é o nome 'coluna_X' e o valor o nome real

    Retorna um dict onde as chaves são nomes finais padrão e os valores são listas de sinônimos.
    """
    resultado = {}
    for i, (grupo_norm, variantes) in enumerate(sugestoes.items(), start=1):
        if len(variantes) == 1:
            nome_final = variantes[0]
        else:
            nome_final = nomes_padrao_manualmente_definidos.get(f"coluna_{i}", f"coluna_{i}")
        resultado[nome_final] = variantes

    return resultado

def encontrar_coluna_padrao(coluna, padrao_colunas):
    for padrao, variantes in padrao_colunas.items():
        if coluna.lower() in [v.lower() for v in variantes]:
            return padrao
    return None

def de_para(spark, directory):

    # Sugere o mapeamento de colunas
    sugestoes = sugerir_de_para(spark, directory,  colunas_exclusivas=["PUlocationID", "DOlocationID", "vendorid"])

    nomes_manualmente_definidos = {
    "coluna_1": "VendorID",
    "coluna_2": "request_datetime",
    "coluna_3": "dropoff_datetime",
    "coluna_4": "total_amount",
    "coluna_5": "passenger_count",
    }

    colunas_fixas = ["PUlocationID", "DOlocationID", "VendorID", "request_datetime"]

 
    de_para_raw = padronizar_de_para(sugestoes, nomes_manualmente_definidos)

    de_para_final = corrigir_de_para_colunas_exclusivas(de_para_raw, colunas_fixas)


    # 4. Exporta para revisar
    exportar_de_para_json(de_para_final, "de_para.json")
    exportar_de_para_csv(de_para_final, "de_para.csv")

    return de_para_final

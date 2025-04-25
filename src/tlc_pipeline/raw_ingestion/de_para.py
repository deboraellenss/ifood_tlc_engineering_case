import logging
import os
import shutil
import json
import csv
import re
from collections import defaultdict
from difflib import get_close_matches
from typing import Dict, List, Optional




def move_to_quarantine(file_path: str, quarantine_path: str = "./quarantine") -> None:
    """
        Move a file to a quarantine directory for further investigation or processing.
        
        Args:
            file_path (str): The path of the file to be moved to quarantine.
            quarantine_path (str, optional): The directory where files will be moved. 
                Defaults to "./quarantine".
        
        Prints a status message upon successful move or if an error occurs during the move operation.
        """
    os.makedirs(quarantine_path, exist_ok=True)
    try:
        shutil.move(
            file_path, os.path.join(quarantine_path, os.path.basename(file_path))
        )
        logging.info(f"🔁 Arquivo movido para quarentena: {file_path}")
    except Exception as e:
        logging.error(f"Erro ao mover {file_path} para quarentena: {e}")


def exportar_de_para_json(de_para_dict: Dict[str, List[str]], caminho_arquivo: str) -> None:
    """Export the mapping dictionary to a JSON file."""
    try:
        with open(caminho_arquivo, "w", encoding="utf-8") as f:
            json.dump(de_para_dict, f, indent=2, ensure_ascii=False)
    except IOError as e:
        logging.error(f"Failed to write to {caminho_arquivo}: {e}")
        raise


def exportar_de_para_csv(de_para_dict, caminho_arquivo):
    with open(caminho_arquivo, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["nome_padrao", "colunas_equivalentes"])
        for nome_padrao, colunas in de_para_dict.items():
            writer.writerow([nome_padrao, ", ".join(colunas)])


def corrigir_de_para_colunas_exclusivas(de_para_dict: Dict[str, List[str]], colunas_exclusivas: List[str]) -> Dict[str, List[str]]:
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

        colunas_obrigatorias = ["tpep_pickup_datetime", "vendorid"]
        for col in colunas_obrigatorias:
            if col not in novo_de_para:
                novo_de_para[col] = [col]

    return novo_de_para


def normalizar_coluna(nome):
    return re.sub(r"[^a-zA-Z0-9]", "", nome.strip().lower())


def sugerir_de_para(
    spark, pasta_parquet: str, limite_similaridade: float = 0.8, colunas_exclusivas: Optional[List[str]] = None
):
    """
        Suggests column mappings by grouping similar columns from Parquet files.
        
        Args:
            spark: Spark session for reading Parquet files
            pasta_parquet: Directory path containing Parquet files
            limite_similaridade: Similarity threshold for column grouping (default 0.8)
            colunas_exclusivas: List of columns to be grouped exclusively (optional)
        
        Returns:
            A dictionary where keys are normalized column names and values are lists of original column names
        """
    error_files = []
    if colunas_exclusivas is None:
        colunas_exclusivas = []

    arquivos = [
        os.path.join(pasta_parquet, f)
        for f in os.listdir(pasta_parquet)
        if f.endswith(".parquet")
    ]
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

        similares = get_close_matches(
            norm, colunas_normalizadas.values(), cutoff=limite_similaridade
        )
        grupo = [orig for orig, n in colunas_normalizadas.items() if n in similares]
        for g in grupo:
            ja_agrupadas.add(g)
        agrupamento[norm].extend(grupo)

    return agrupamento


def padronizar_de_para(sugestoes: dict[str, list[str]], nomes_padrao_manualmente_definidos: dict[str, str] | None = None):
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
            nome_final = nomes_padrao_manualmente_definidos.get(
                f"coluna_{i}", f"coluna_{i}"
            )
        resultado[nome_final] = variantes

    return resultado


def encontrar_coluna_padrao(coluna: str, padrao_colunas: dict[str, list[str]]) -> str | None:
    for padrao, variantes in padrao_colunas.items():
        if coluna.lower() in [v.lower() for v in variantes]:
            return padrao
    return None


def de_para(spark, directory: str):
    """
    Generates a standardized column mapping for raw data ingestion.

    Args:
    spark: Active Spark session for reading Parquet files
    directory (str): Path to the directory containing Parquet files to analyze

    Returns:
        dict: A dictionary of standardized column mappings where keys are standard column names 
            and values are lists of equivalent column variants

    Exports:
        - de_para.json: JSON file with column mapping
        - de_para.csv: CSV file with column mapping

    This function performs the following steps:
    1. Suggests column mappings using sugerir_de_para()
    2. Applies manual column name definitions
    3. Corrects mappings for exclusive columns
    4. Exports the mapping to JSON and CSV files
    5. Logs intermediate mapping details
    """

    # Sugere o mapeamento de colunas
    sugestoes = sugerir_de_para(
        spark,
        directory,
        colunas_exclusivas=[
            "PUlocationID",
            "DOlocationID",
            "vendorid",
            "tpep_pickup_datetime",
        ],
    )

    nomes_manualmente_definidos = {
        "coluna_1": "VendorID",
        "coluna_2": "request_datetime",
        "coluna_3": "dropoff_datetime",
        "coluna_4": "total_amount",
        "coluna_5": "passenger_count",
    }

    colunas_fixas = [
        "PUlocationID",
        "DOlocationID",
        "VendorID",
        "request_datetime",
        "tpep_pickup_datetime",
    ]

    de_para_raw = padronizar_de_para(sugestoes, nomes_manualmente_definidos)

    de_para_final = corrigir_de_para_colunas_exclusivas(de_para_raw, colunas_fixas)

    # 4. Exporta para revisar
    exportar_de_para_json(de_para_final, "de_para.json")
    exportar_de_para_csv(de_para_final, "de_para.csv")
    logging.info("🔁🔁🔁🔁De para exportado para de_para.json e de_para.csv")

    logging.info(f"De-para sugerido:{sugestoes}")


    logging.info(f"De-para final corrigido:{de_para_raw}")


    logging.info(f"Colunas finais esperadas:{de_para_final}")


    return de_para_final

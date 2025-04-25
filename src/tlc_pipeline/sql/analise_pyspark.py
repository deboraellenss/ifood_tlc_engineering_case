from utils.helpers import get_spark_session
from config import OUTPUT_BUCKET


def main():
    spark = get_spark_session(app_name="taxi_analysis")

    # Caminho para seus dados Parquet (silver)
    silver_path = "file:///caminho/completo/silver/yellow_taxi/"

    # Criação da tabela Hive externa (se ainda não existir)
    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS yellow_taxi (
            VendorID INT,
            passenger_count INT,
            total_amount DOUBLE,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP
        )
        PARTITIONED BY (year STRING, month STRING)
        STORED AS PARQUET
        LOCATION '{OUTPUT_BUCKET}'
    """
    )

    # Atualiza as partições da tabela com base no diretório
    spark.sql("MSCK REPAIR TABLE yellow_taxi")

    # ---------------------
    # 1. Valor médio arrecadado por mês
    print("\n📊 Valor médio arrecadado por mês:")
    resultado1 = spark.sql(
        """
        SELECT
            year,
            month,
            ROUND(AVG(total_amount), 2) AS media_valor_arrecadado
        FROM yellow_taxi
        GROUP BY year, month
        ORDER BY year, month
    """
    )
    resultado1.show(truncate=False)

    # ---------------------
    # 2. Média de passageiros por hora e por dia
    print("\n👥 Média de passageiros por hora e por dia:")
    resultado2 = spark.sql(
        """
        SELECT
            DATE(tpep_pickup_datetime) AS dia,
            HOUR(tpep_pickup_datetime) AS hora,
            ROUND(AVG(passenger_count), 2) AS media_passageiros
        FROM yellow_taxi
        GROUP BY dia, hora
        ORDER BY dia, hora
    """
    )
    resultado2.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

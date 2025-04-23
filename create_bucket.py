import io
from minio import Minio

client = Minio(
    "localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
)


bucket = "yellow_taxi_files"


if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"✅ Bucket '{bucket}' criado!")
else:
    print(f"⚠️ Bucket '{bucket}' já existe.")


print("✅ Setup do bucket finalizado com sucesso!")

run: up create write

up:
	@echo "🚀 Subindo o MinIO com Docker Compose..."
	docker-compose up -d


create:
	@echo "🪣 Criando bucket no MinIO..."
	python create_bucket.py

write:
	@echo "✍️ Gravando dados no bucket via PySpark..."
	python write_spark.py

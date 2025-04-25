# 🚕 Pipeline de Processamento de Dados TLC

## 🌟 Visão Geral do Projeto

Este projeto implementa um pipeline de dados completo para processar os dados de viagens da Comissão de Táxi e Limusine (TLC) de Nova York. O pipeline abrange desde a ingestão de dados brutos até a criação de camadas bronze e silver, com orquestração via Airflow e tabelas Hive para análise.

## 🏗️ Arquitetura do Pipeline

O pipeline segue uma arquitetura de data lakehouse com múltiplas camadas:

1. **📥 Camada Raw (Ingestão)**: Lê arquivos de dados de origem (formato Parquet)
2. **🥉 Camada Bronze**: Dados normalizados com esquema padronizado
3. **🥈 Camada Silver**: Dados enriquecidos e transformados para análise
4. **📊 Camada de Análise**: Tabelas Hive para consultas analíticas

## 🚀 Como Executar o Projeto

### 📋 Pré-requisitos

- Python 3.8+
- Docker e Docker Compose
- Make

### 🔧 Configuração do Ambiente

#### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/tlc-pipeline.git
cd tlc-pipeline
```

#### 2. Crie e ative um ambiente virtual

```bash
python -m venv venv
source venv/bin/activate  # No Windows: venv\Scripts\activate
```

#### 3. Instale o projeto como pacote

```bash
pip install -e .
```

#### 4. Inicie o ambiente de simulação AWS com MinIO

```bash
make up
```

Este comando iniciará um contêiner Docker com MinIO que simula o S3 da AWS.

#### 5. 🪣 Crie bucket no MinIO

```bash
make create
```

Este comando configurará as credenciais necessárias para acessar o MinIO.

### 🏃‍♂️ Executando o Pipeline

#### 1. Execute o pipeline completo

```bash
make run-pipeline
```

#### 2. Execute apenas a camada bronze

```bash
make run-bronze
```

#### 3. Execute apenas a camada silver

```bash
make run-silver
```

### 🧪 Executando Testes

```bash
make test
```

### 🧹 Limpando o Ambiente

```bash
make clean
```

Este comando removerá os contêineres Docker e limpará os dados temporários.

## 🧩 Componentes Principais

### 📥 Ingestão de Dados Brutos

O processo de ingestão bruta lê arquivos Parquet de um local de origem e os prepara para processamento adicional.

#### 🔄 Mapeamento de Colunas (De-Para)

O pipeline inclui um sistema sofisticado de mapeamento de colunas que:

- 📝 Normaliza nomes de colunas em diferentes fontes de dados
- 🔍 Trata colunas especiais/exclusivas que precisam de tratamento individual
- 🔗 Agrupa colunas semelhantes com base em métricas de similaridade

```python
# Exemplo de uso:
de_para = sugerir_de_para(spark, "caminho/para/arquivos/parquet", limite_similaridade=0.8)
```

#### 🛠️ Funções de Mapeamento de Colunas

1. **`sugerir_de_para()`**: Analisa arquivos Parquet para sugerir mapeamentos de colunas com base na similaridade
  - 🚫 Trata arquivos com erro movendo-os para quarentena
  - 🏷️ Suporta colunas exclusivas que não devem ser agrupadas
  - 🔍 Usa correspondência aproximada para identificar colunas semelhantes

2. **`corrigir_de_para_colunas_exclusivas()`**: Refina mapeamentos de colunas separando colunas exclusivas
  - ✅ Garante que colunas obrigatórias como "tpep_pickup_datetime" e "vendorid" sejam incluídas
  - 🔀 Cria mapeamentos individuais para colunas exclusivas

3. **`exportar_de_para_json()`**: Exporta mapeamentos de colunas para formato JSON para referência e auditoria

### 📊 Normalização de Esquema

A função `ler_e_padronizar_parquets()` é responsável por:

- 📋 Ler múltiplos arquivos Parquet com esquemas diferentes
- 🔄 Aplicar o mapeamento de colunas para padronizar os nomes
- 🏷️ Adicionar metadados como tipo de fonte e arquivo de origem
- 🔍 Extrair informações de data a partir dos nomes dos arquivos

### 🥉 Camada Bronze

A camada bronze processa os dados brutos e:

- 🧹 Limpa e padroniza os dados
- 📊 Aplica o esquema consistente em todos os dados
- 🔍 Adiciona metadados de processamento
- 📂 Organiza os dados em partições eficientes

### 🥈 Camada Silver

A camada silver enriquece os dados da camada bronze:

- 🔍 Aplica regras de negócio e transformações
- 🧮 Calcula métricas e agregações
- 🔗 Cria relacionamentos entre diferentes conjuntos de dados
- 📊 Prepara os dados para análise

### 📝 SQL para Tabelas Hive

A pasta SQL contém scripts para:

- 📊 Criar tabelas externas Hive apontando para os dados processados
- 🔍 Definir particionamento e esquemas para consultas eficientes
- 🧮 Criar views para facilitar análises comuns

### 🔄 Orquestração com Airflow

Os DAGs do Airflow orquestram todo o pipeline:

- ⏱️ Agendamento de execuções periódicas
- 🔄 Gerenciamento de dependências entre tarefas
- 📊 Monitoramento de execução
- ⚠️ Tratamento de falhas e retentativas

### 💾 Escrita de Dados

A função `write_to_raw_bucket()` lida com a escrita de dados processados no armazenamento de destino:

- 📊 Particiona dados por tipo de fonte, ano e mês para consultas eficientes
- 🛡️ Trata dataframes vazios adequadamente
- ⚠️ Fornece tratamento de erros e registro de logs
- 🔄 Suporta caminhos locais e S3

## 🔄 Fluxo de Dados Completo

1. 📥 **Ingestão**: Arquivos Parquet de origem são lidos
2. 🔄 **Normalização**: Esquemas são padronizados usando o sistema de-para
3. 🥉 **Bronze**: Dados normalizados são escritos na camada bronze
4. 🥈 **Silver**: Dados são transformados e enriquecidos na camada silver
5. 📊 **Análise**: Tabelas Hive são criadas ou atualizadas para consulta

## ⚙️ Automação com Makefile

O projeto inclui um Makefile para automatizar tarefas comuns:

- 🔧 Configuração do ambiente de desenvolvimento
- 🧪 Execução de testes
- 📦 Construção de pacotes
- 🚀 Implantação em diferentes ambientes

```bash
# Exemplos de comandos make
make setup      # Configura o ambiente
make test       # Executa testes
make build      # Constrói o pacote
make deploy     # Implanta o pipeline
```

## ⚠️ Tratamento de Erros

O pipeline inclui tratamento robusto de erros:
- 🔒 Arquivos que não podem ser processados são movidos para um diretório de quarentena
- 📝 Mensagens de erro detalhadas são registradas
- 🛡️ Dataframes vazios são tratados adequadamente
- 🔄 Falhas em DAGs são tratadas com políticas de retentativa

## 🚀 Exemplos de Uso

```python
# 1. Inicializar sessão Spark
spark = SparkSession.builder.appName("TLC Pipeline").getOrCreate()

# 2. Gerar mapeamentos de colunas
de_para = sugerir_de_para(spark, "dados_entrada/", 
                        limite_similaridade=0.8,
                        colunas_exclusivas=["coluna_especial1", "coluna_especial2"])

# 3. Refinar mapeamentos para colunas exclusivas
de_para_refinado = corrigir_de_para_colunas_exclusivas(de_para, 
                                                    colunas_exclusivas=["coluna_especial1", "coluna_especial2"])

# 4. Exportar mapeamentos para referência
exportar_de_para_json(de_para_refinado, "mapeamentos/mapeamento_colunas.json")

# 5. Ler e padronizar os dados
df_padronizado = ler_e_padronizar_parquets(spark, "dados_entrada/", de_para_refinado)

# 6. Processar e escrever dados na camada bronze
write_to_raw_bucket(df_padronizado, "s3://bucket-bronze/dados_tlc/")

# 7. Transformar para camada silver
df_silver = transformar_para_silver(df_padronizado)
write_to_silver(df_silver, "s3://bucket-silver/dados_tlc/")
```

## 💡 Melhores Práticas

- 👀 Sempre revise os mapeamentos de colunas gerados antes de usá-los em produção
- 🔍 Monitore o diretório de quarentena para arquivos problemáticos
- 🎛️ Ajuste o limite de similaridade com base nas características dos seus dados
- ✅ Inclua todas as colunas obrigatórias em seu mapeamento
- 📊 Verifique a qualidade dos dados após o processamento
- 🔄 Use o Airflow para monitorar a execução do pipeline

## 📊 Estrutura do Projeto

```
.
├── Makefile                      # Automação de tarefas
├── README.md                     # Documentação do projeto
├── dags/                         # DAGs do Airflow para orquestração
│   ├── dag.py         # DAG para processamento das camada bronze e silver
├── sql/                          # Scripts SQL para tabelas Hive
│   ├── create_table_hive.sql     # Criação de tabelas na camada silver
└── src/
   └── tlc_pipeline/
       ├── bronze/               # Processamento da camada bronze
       │   └── bronze_layer.py
       ├── raw_ingestion/        # Ingestão de dados brutos
       │   ├── de_para.py        # Funcionalidade de mapeamento de colunas
       │   ├── normalize_schema.py # Normalização de esquemas
       │   └── write_to_raw_bucket.py # Funcionalidade de escrita de dados
       └── silver/               # Processamento da camada silver
           └── silver_layer.py
```

## 📝 Licença

Este projeto está licenciado sob a Licença MIT - consulte o arquivo LICENSE para obter detalhes.
```

Para criar este arquivo, execute:

```bash
mkdir -p $(dirname README.md)
touch README.md
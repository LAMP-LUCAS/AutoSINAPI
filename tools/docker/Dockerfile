# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Instala o Git
RUN apt-get update && apt-get install -y git

# Define o diretorio de trabalho dentro do container
WORKDIR /app

# Copia todo o contexto do projeto para o diretorio de trabalho
COPY . /app

# Atualiza o pip e instala o driver do postgres explicitamente
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir psycopg2-binary

# Instala as dependencias do projeto
RUN pip install --no-cache-dir .

# Define o comando padrao para executar o pipeline
CMD ["python", "tools/autosinapi_pipeline.py"]
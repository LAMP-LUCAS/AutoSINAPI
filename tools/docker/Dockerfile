# Use uma imagem base oficial do Python
FROM python:3.9-slim

# Instala o Git
RUN apt-get update && apt-get install -y git

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia todo o contexto do projeto para o diretório de trabalho
# O .dockerignore na raiz do projeto irá controlar o que é copiado
COPY . /app

# Instala as dependências do projeto
# Agora o setuptools-scm funcionará, pois o Git está instalado
RUN pip install --no-cache-dir .

# Define o comando padrão para executar o pipeline
CMD ["python", "tools/autosinapi_pipeline.py"]

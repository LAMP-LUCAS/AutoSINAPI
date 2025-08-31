# Makefile para gerenciar o ambiente Docker do AutoSINAPI

.PHONY: build up run down clean

# Constrói ou reconstrói a imagem da aplicação se houver mudanças
build:
	@echo "=> Construindo a imagem do AutoSINAPI..."
	docker-compose build

# Sobe os serviços (como o banco de dados) em background
up:
	@echo "=> Iniciando serviços em background (PostgreSQL)..."
	docker-compose up -d db

# Executa o pipeline. Garante que a imagem está construída e os serviços de pé.
run: build up
	@echo "=> Executando o pipeline do AutoSINAPI..."
	docker-compose run --rm app

# Para e remove os containers
down:
	@echo "=> Parando e removendo os containers..."
	docker-compose down

# Limpa tudo: containers e o volume do banco de dados (cuidado!)
clean: down
	@echo "=> Removendo o volume do banco de dados..."
	docker volume rm tools_docker_postgres_data

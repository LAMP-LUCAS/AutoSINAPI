# AutoSINAPI

Este repositório tem como objetivo o desenvolvimento open source de uma solução para captação, tratamento e inserção dos dados do SINAPI (Sistema Nacional de Pesquisa de Custos e Índices da Construção Civil) em um banco de dados PostgreSQL de forma estruturada, editável e atualizável de maneira autônoma. Atualmente está realizando os seus objetivos, porém busca-se criar uma api RESTFULL para facilitar a consulta desses dados e uma integração com a API SINCRO para captar e tratar os dados SINAPI em vários fornecedores mantendo um bypass e segurança em seus consumos. Eu te convido a participar desse projeto e dos outros incluídos no foton, veja mais no repositório FOTON aqui no Github: [FOTON](https://github.com/LAMP-LUCAS/foton)

Se você não tem costume ou nunca utilizou código aqui do github eu pedi ao DeepSeek para fazer um guia, passo a passo aqui neste link: [GUIA]

Se você nunca usou o DeepSeek ou outro LLM (Large Languange Model - Grande Modelo de Linguagem) para te ajudar desenvolver soluções do dia-a-dia de projetos e obras, te convido a entrar no grupo de estudos aqui no redmine: [TUTORIAL-INICIO](https://github.com/LAMP-LUCAS/AutoSINAPI/tree/postgres_data-define/docs/TUTORIAL-INICIO.md)

## Objetivos

- Automatizar o download dos dados do SINAPI
- Tratar e organizar os dados para facilitar consultas e análises
- Inserir os dados em um banco PostgreSQL, permitindo edição e atualização recorrente
- Prover scripts e ferramentas para facilitar a manutenção e evolução do processo

## Estrutura do Projeto

```plaintext
├── autosinapi_pipeline.py    # Script Exemplo para download, tratamento e insersão dos arquivos SINAPI no banco de dados
├── CONFIG.json      # Arquivo de configuração para automatização do pipeline 
├── sinap_webscraping_download_log.json      # Arquivo de registro dos downloads
├── sql_access.secrets      # Arquivo de configuração do banco (exemplo) - Retirar ".example"
├── sinapi_utils.py      # Módulo contendo toda lógica do projeto
├── update_requirements.py  # Atualizador de dependências
├── setup.py    # Configuração do módulo
├── pyproject.toml    # Configuração do módulo
└── requirements.txt        # Dependências do projeto
```

## Configuração Inicial

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/AutoSINAPIpostgres.git
cd AutoSINAPIpostgres
```

### 2. Configure o ambiente virtual Python

```bash
python -m venv venv
.\venv\Scripts\activate
```

### 3. Instale as dependências

```bash
python update_requirements.py  # Gera requirements.txt atualizado, OPCIONAL!
pip install -r requirements.txt
```

### 4. Configure o acesso ao PostgreSQL

- Renomeie `sql_access.secrets.example` para `sql_access.secrets`
- Edite o arquivo com suas credenciais:

```ini
DB_USER = 'seu_usuario'
DB_PASSWORD = 'sua_senha'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'sinapi'
DB_INITIAL_DB = 'postgres'
```

### 5. Configure o arquivo CONFIG.json para automatização das etapas

- Atualmente está configurado para tratar os dados das bases à partir de 2025, substituindo os dados antigos e utilizando o arquivo XLSX REFERENCIA para insersão:

```ini
{
    "secrets_path": "sql_access.secrets", # arquivo com os parâmetros de conexão
    "default_year": "2025", # ano da base desejada
    "default_month": "01", # mês da base desejada
    "default_format": "xlsx", # formato de arquivo a ser trabalhado (Atualmente só suporta XLSX)
    "workbook_type_name": "REFERENCIA", # Workbook exemplo para trabalhar
    "duplicate_policy": "substituir", # Política de insersão de dados novos
    "backup_dir": "./backups", # Pasta para salvamento dos dados tratados antes de inserir no banco de dados
    "log_level": "info", # Nível de LOG
    "sheet_processors": { # Configuração de recorte de dados para cada tipo de planilha {NOME_PLANILHA: {COLUNA_RECORTE, COLUNA_CABEÇALHO}}
        "ISD": {"split_id": 5, "header_id": 9},
        "CSD": {"split_id": 4, "header_id": 9},
        "ANALITICO": {"split_id": 0, "header_id": 9},
        "COEFICIENTES": {"split_id": 5, "header_id": 5},
        "MANUTENCOES": {"split_id": 0, "header_id": 5},
        "MAO_DE_OBRA": {"split_id": 4, "header_id": 5}
    }
}
```

## Uso dos Scripts

### 1. Download de Dados SINAPI

O script `autosinap_pipeline.py` realiza todas as etapas necessárias para o download dos arquivos do SINAPI e insersão no banco de dados PostgreSQL:

```bash
python autosinap_pipeline.py
```

Se não configurar o CONFIG.json Você será solicitado a informar:

- Ano (YYYY)
- Mês (MM)
- Tipo de planilha (familias_e_coeficientes, Manutenções, mao_de_obra, Referência)
- Formato (xlsx é o único formato suportado até o momento)

### >> FUTURA IMPLANTAÇÃO << CLI para o scripy PostgreSQL

O script `autosinapi_cli_pipeline.py` processa e insere os dados no banco:

```bash
python autosinapi_cli_pipeline.py --arquivo_xlsx <caminho> --tipo_base <tipo> --config <caminho>
```

Parâmetros disponíveis:

- `--arquivo_xlsx`: Caminho do arquivo Excel a ser processado
- `--config`: Caminho do arquivo de configuração CONFIG.json
- `--tipo_base`: Tipo de dados (insumos, composicao, analitico)
- `--user`: Usuário do PostgreSQL (opcional, usa .secrets se não informado)
- `--password`: Senha do PostgreSQL (opcional, usa .secrets se não informado)
- `--host`: Host do PostgreSQL (opcional, usa .secrets se não informado)
- `--port`: Porta do PostgreSQL (opcional, usa .secrets se não informado)
- `--dbname`: Nome do banco (opcional, usa .secrets se não informado)

## Estrutura do Banco de Dados

O banco PostgreSQL é organizado em schemas por tipo de dados:

- `insumos`: Preços e informações de insumos
- `composicoes`: Composições de serviços
- `analitico`: Dados analíticos detalhados

## Troubleshooting

### Erros Comuns

1. Erro de conexão PostgreSQL:
   - Verifique se o PostgreSQL está rodando
   - Confirme as credenciais em `sql_access.secrets`
   - Verifique se o banco e schemas existem ou se foram criados corretamente pelo script `autosinapi_pipeline.py`

2. Erro no download SINAPI:
   - Verifique sua conexão com a internet
   - Confirme se o arquivo existe no site da Caixa
   - Verifique o formato do ano (YYYY) e mês (MM)
   - ATENÇÃO: Se realizadas várias tentativas a plataforma da CEF pode bloquear seu IP, utilize próxies ou aguarde um tempo antes de tentar novamente.

3. Erro na análise Excel:
   - Confirme se o arquivo não está aberto em outro programa
   - Verifique se há permissão de leitura no diretório
   - Verifique se as configurações de split e header presentes no arquivo `CONFIG.json` estão corretas

## Como contribuir

1. Faça um fork deste repositório
2. Crie uma branch para sua feature ou correção
3. Envie um pull request detalhando as alterações propostas
4. Beba água e se possível passe um cafezinho antes de contribuir.

## Requisitos do Sistema

- Python 3.0+
- PostgreSQL 12+
- Bibliotecas Python listadas em `requirements.txt`

## Licença

Este projeto é open source sob os termos da GNU General Public License, versão 3 (GPLv3). Isso significa que você pode utilizar, modificar e distribuir o projeto, inclusive para fins comerciais. Contudo, se você criar derivados ou incorporar este código em outros produtos e distribuí-los, estes também deverão estar sob licença GPLv3, garantindo assim que o código-fonte continue acessível aos usuários.

## Contato

Sugestões, dúvidas ou colaborações são bem-vindas via issues ou pull requests.

## Árvore de configuração do diretório

```plaintext
📦AutoSINAPI
 ┣ 📂autosinapi.egg-info
 ┃ ┣ 📜dependency_links.txt
 ┃ ┣ 📜PKG-INFO
 ┃ ┣ 📜requires.txt
 ┃ ┣ 📜SOURCES.txt
 ┃ ┗ 📜top_level.txt
 ┣ 📂docs # Documentação do projeto >> Irá ser implantado juntamente com um forum/comunidade em um redmine
 ┣ 📂tests # Local especial para testar modificações e implantações sem quebrar todo o resto :)
 ┣ 📂tools # Ferramentas que podem ser criadas utilizando este módulo
 ┃ ┃ ┣ 📂downloads # local onde serão salvos os downloads do script
 ┃ ┣ 📜autosinapi_pipeline.py
 ┃ ┣ 📜CONFIG.json
 ┃ ┣ 📜sinap_webscraping_download_log.json
 ┃ ┣ 📜sql_access copy.secrets.example
 ┃ ┗ 📜__init__.py
 ┣ 📜.gitignore
 ┣ 📜pyproject.toml
 ┣ 📜README.md
 ┣ 📜requirements.txt
 ┣ 📜setup.py
 ┣ 📜sinapi_utils.py
 ┣ 📜update_requirements.py
 ┗ 📜__init__.py
```

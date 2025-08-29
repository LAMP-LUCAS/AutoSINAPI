
# 🔄 AutoSINAPI: Pipeline e Toolkit para Dados SINAPI

**Solução open source para simplificar o acesso, tratamento e gestão dos dados do SINAPI (Sistema Nacional de Pesquisa de Custos e Índices da Construção Civil).**

O AutoSINAPI transforma planilhas reais do SINAPI em dados estruturados, validados e prontos para análise ou integração com bancos PostgreSQL, APIs e dashboards. O projeto segue Clean Code, SOLID e boas práticas de testes automatizados.


## � Principais Funcionalidades

| Funcionalidade                  | Status       | Próximos Passos              |
|---------------------------------|--------------|------------------------------|
| Download automático do SINAPI   | ✅ Funcional | API REST para consultas      |
| Processamento robusto de planilhas reais | ✅ Implementado | Integração com SINCRO API  |
| Inserção em PostgreSQL          | ✅ Operante  | Dashboard de análises        |
| CLI para pipeline               | 🚧 Em desenvolvimento | Documentação interativa |

---


## 🏗️ Arquitetura e Organização

O AutoSINAPI é dividido em módulos desacoplados:

- **core/**: processamento, download, validação e integração com banco
- **tools/**: scripts CLI e utilitários
- **tests/**: testes unitários e de integração (pytest, mocks, arquivos reais)
- **docs/**: documentação técnica, DataModel, tutorial e padrões

O pipeline segue o modelo ETL (Extração, Transformação, Carga) e pode ser usado como biblioteca Python ou via CLI.

### Modelo de Dados
O modelo relacional segue o DataModel descrito em [`docs/DataModel.md`](docs/DataModel.md), cobrindo:
- Catálogo de insumos e composições
- Séries históricas de preços/custos
- Estrutura de composições e histórico de manutenções

## 🌟 Por Que Contribuir?

- **Impacto direto** na gestão de custos da construção civil
- Ambiente **amigável para iniciantes** em programação
- **Aprendizado prático** com Python, PostgreSQL e automação
- Faça parte de uma comunidade que **simplifica dados complexos!**

> "Sozinhos vamos mais rápido, juntos vamos mais longe" - Venha construir esta solução conosco! 🏗️💙
## Objetivos

- Automatizar o download dos dados do SINAPI
- Tratar e organizar os dados para facilitar consultas e análises
- Inserir os dados em um banco PostgreSQL, permitindo edição e atualização recorrente
- Prover scripts e ferramentas para facilitar a manutenção e evolução do processo


## 📂 Estrutura do Projeto

```plaintext
AutoSINAPI/
 ┣ autosinapi/           # Código principal (core, pipeline, config, exceptions)
 ┣ tools/                # Scripts CLI, downloads, configs de exemplo
 ┣ tests/                # Testes unitários e integração (pytest, arquivos reais e sintéticos)
 ┣ docs/                 # Documentação, DataModel, tutorial, nomenclaturas
 ┣ requirements.txt      # Dependências
 ┣ pyproject.toml        # Configuração do módulo
 ┣ setup.py              # Instalação
 ┗ README.md
```


## ⚙️ Instalação e Configuração


### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/AutoSINAPIpostgres.git
cd AutoSINAPIpostgres
```


### 2. Crie e ative o ambiente virtual Python

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


### 5. Configure o arquivo CONFIG.json (opcional para uso local)

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


## 🛠️ Uso dos Scripts


### 1. Pipeline completo (download, processamento, inserção)


O script `tools/autosinapi_pipeline.py` realiza todas as etapas necessárias para o download dos arquivos do SINAPI e inserção no banco de dados PostgreSQL:

```bash
python autosinap_pipeline.py
```

Se não configurar o CONFIG.json Você será solicitado a informar:

- Ano (YYYY)
- Mês (MM)
- Tipo de planilha (familias_e_coeficientes, Manutenções, mao_de_obra, Referência)
- Formato (xlsx é o único formato suportado até o momento)


### 2. (Futuro) CLI para processamento customizado

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


## 🗄️ Estrutura do Banco de Dados

O modelo segue o DataModel do projeto, com tabelas para insumos, composições, preços, custos, estrutura e histórico. Veja [`docs/DataModel.md`](docs/DataModel.md) para detalhes e exemplos.


## 🩺 Troubleshooting

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


## 🤝 Como contribuir

1. Faça um fork deste repositório
2. Crie uma branch para sua feature ou correção
3. Envie um pull request detalhando as alterações propostas
4. Beba água e se possível passe um cafezinho antes de contribuir.


## 💻 Requisitos do Sistema

- Python 3.0+
- PostgreSQL 12+
- Bibliotecas Python listadas em `requirements.txt`


## 📝 Licença

Este projeto é open source sob os termos da GNU General Public License, versão 3 (GPLv3). Isso significa que você pode utilizar, modificar e distribuir o projeto, inclusive para fins comerciais. Contudo, se você criar derivados ou incorporar este código em outros produtos e distribuí-los, estes também deverão estar sob licença GPLv3, garantindo assim que o código-fonte continue acessível aos usuários.


## 📬 Contato

Sugestões, dúvidas ou colaborações são bem-vindas via issues ou pull requests.


---

> Para detalhes sobre arquitetura, padrões, DataModel e roadmap, consulte a pasta [`docs/`](docs/).

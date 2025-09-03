# Plano de Trabalho e Roadmap do Módulo AutoSINAPI

Este documento serve como um guia central para o desenvolvimento, acompanhamento e verificação das entregas do módulo `AutoSINAPI`. Ele define a arquitetura, a interface pública e o caminho a ser seguido.

## 1. Objetivos e Entregas Principais

O objetivo final é transformar o `AutoSINAPI` em uma biblioteca Python (`toolkit`) robusta, testável e desacoplada, pronta para ser consumida por outras aplicações, como uma API REST ou uma CLI.

As entregas incluem:
- **Pipeline ETL**: Processamento completo de arquivos do SINAPI, aderente ao `DataModel.md`.
- **Cobertura de Testes**: Testes unitários e de integração automatizados.
- **Interface Pública**: Uma função `run_etl()` clara e padronizada.
- **Arquitetura Modular**: Código organizado em módulos com responsabilidades únicas (`downloader`, `processor`, `database`).
- **Documentação**: Manuais de uso, arquitetura e contribuição.

## 2. Status Geral (Visão Macro)

Use esta seção para um acompanhamento rápido do progresso geral.

- [ ] **Fase 1**: Refatoração do Módulo para Toolkit
- [ ] **Fase 2**: Cobertura de Testes Unitários e de Integração
- [ ] **Fase 3**: Empacotamento e Documentação Final
- [ ] **Fase 4**: Implementação da API e CLI (Pós-Toolkit)

---

## 3. Visão Geral da Arquitetura

A nossa arquitetura será baseada em **desacoplamento**. A API não executará o pesado processo de ETL diretamente. Em vez disso, ela atuará como um **controlador**, delegando a tarefa para um **trabalhador (worker)** em segundo plano. O módulo `AutoSINAPI` será o **toolkit** que o trabalhador utilizará.

**Diagrama da Arquitetura:**

```
+-----------+     +----------------+     +----------------+     +---------------------+
|           |     |                |     |                |     |  API FastAPI        |
| Usuário   |---->| Kong Gateway   |---->|  (Controller)  |---->| (Fila de Tarefas)   |
| (Admin)   |     | (Auth & Proxy) |     |  POST /populate|     |  Ex: Redis          |
+-----------+     +----------------+     +----------------+     +----------+----------+
                                                                             |
                                                                    (Nova Tarefa)
                                                                             |
                                                                             v
+-------------------------------------------------+     +--------------------+----------+
|  AutoSINAPI Toolkit                             |<----|                               |
| (Biblioteca Python instalada via pip)           |     |  Trabalhador (Celery Worker)  |
| - Lógica de Download (em memória/disco)         |     |  - Pega tarefa da fila        |
| - Lógica de Processamento (pandas)              |     |  - Executa a lógica do        |
| - Lógica de Banco de Dados (SQLAlchemy)         |     |    AutoSINAPI Toolkit         |
+-------------------------------------------------+     +--------------------+----------+
                                                                             |
                                                                 (Escreve os dados)
                                                                             |
                                                                             v
                                                                 +--------------------+
                                                                 |                    |
                                                                 |  Banco de Dados    |
                                                                 |  (PostgreSQL)      |
                                                                 +--------------------+
```

-----

## 4. O Contrato do Toolkit (Interface Pública)

Para que o `AutoSINAPI` seja consumível por outras aplicações, ele deve expor uma interface clara e previsível.

#### **Requisito 1: A Interface Pública do Módulo**

O `AutoSINAPI` deverá expor, no mínimo, uma função principal, clara e bem definida.

**Função Principal Exigida:**
`autosinapi.run_etl(db_config: dict, sinapi_config: dict, mode: str)`

  * **`db_config (dict)`**: Um dicionário contendo **toda** a informação de conexão com o banco de dados. A API irá montar este dicionário a partir das suas próprias variáveis de ambiente (`.env`).
    ```python
    # Exemplo de db_config que a API irá passar
    db_config = {
        "user": "admin",
        "password": "senha_super_secreta",
        "host": "db",
        "port": 5432,
        "dbname": "sinapi"
    }
    ```
  * **`sinapi_config (dict)`**: Um dicionário com os parâmetros da operação. A API também montará este dicionário.
    ```python
    # Exemplo de sinapi_config que a API irá passar
    sinapi_config = {
        "year": 2025,
        "month": 8,
        "workbook_type": "REFERENCIA",
        "duplicate_policy": "substituir" 
    }
    ```
  * **`mode (str)`**: O seletor de modo de operação.
      * `'server'`: Ativa o modo de alta performance, com todas as operações em memória (bypass de disco).
      * `'local'`: Usa o modo padrão, salvando arquivos em disco, para uso pela comunidade.

#### **Requisito 2: Lógica de Configuração Inteligente (Sem Leitura de Arquivos)**

Quando usado como biblioteca (`mode='server'`), o módulo `AutoSINAPI`:

  * **NÃO PODE** ler `sql_access.secrets` ou `CONFIG.json`.
  * **DEVE** usar exclusivamente os dicionários `db_config` e `sinapi_config` passados como argumentos.
  * Quando usado em modo `local`, ele pode manter a lógica de ler arquivos `CONFIG.json` para facilitar a vida do usuário final que o clona do GitHub.

#### **Requisito 3: Retorno e Tratamento de Erros**

A função `run_etl` deve retornar um dicionário com o status da operação e levantar exceções específicas para que a API possa tratar os erros de forma inteligente.

  * **Retorno em caso de sucesso:**
    ```python
    {"status": "success", "message": "Dados de 08/2025 populados.", "tables_updated": ["insumos_isd", "composicoes_csd"]}
    ```
  * **Exceções:** O módulo deve definir e levantar exceções customizadas, como `autosinapi.exceptions.DownloadError` ou `autosinapi.exceptions.DatabaseError`.

-----

## 5. Roadmap de Desenvolvimento (Etapas Detalhadas)

Este é o plano de ação detalhado, dividido em fases e tarefas.

### Fase 1: Evolução do `AutoSINAPI` para um Toolkit

Esta fase é sobre preparar o módulo para ser consumido pela nossa API.

  * **Etapa 1.1: Refatoração Estrutural:** Quebrar o `sinapi_utils.py` em módulos menores (`downloader.py`, `processor.py`, `database.py`) dentro de uma estrutura de pacote Python, como planejamos anteriormente.
  * **Etapa 1.2: Implementar a Lógica de Configuração Centralizada:** Remover toda a leitura de arquivos de configuração de dentro das classes e fazer com que elas recebam suas configurações via construtor (`__init__`).
  * **Etapa 1.3: Criar a Interface Pública:** Criar a função `run_etl(db_config, sinapi_config, mode)` que orquestra as chamadas para as classes internas.

    * **Etapa 1.3.1: Desacoplar as Classes (Injeção de Dependência):** Em vez de uma classe criar outra (ex: `self.downloader = SinapiDownloader()`), ela deve recebê-la como um parâmetro em seu construtor (`__init__(self, downloader)`). Isso torna o código muito mais flexível e testável.
  * **Etapa 1.4: Implementar o Modo Duplo:** Dentro das classes `downloader` e `processor`, adicionar a lógica `if mode == 'server': ... else: ...` para lidar com operações em memória vs. em disco.
  * **Etapa 1.5: Empacotamento:** Garantir que o módulo seja instalável via `pip` com um `setup.py` ou `pyproject.toml`.

**Estrutura de Diretórios Alvo:**

```
/AutoSINAPI/
├── autosinapi/             # <--- NOVO: O código da biblioteca em si
│   ├── core/               # <--- Lógica de negócio principal
│   │   ├── database.py     #      (antiga classe DatabaseManager)
│   │   ├── downloader.py   #      (antiga classe SinapiDownloader)
│   │   ├── processor.py    #      (classes ExcelProcessor, SinapiProcessor)
│   │   └── file_manager.py #      (antiga classe FileManager)
│   ├── pipeline.py         #      (antiga classe SinapiPipeline)
│   ├── config.py           #      (Nova lógica para carregar configs do .env)
│   ├── exceptions.py       #      (Definir exceções customizadas, ex: DownloadError)
│   └── __init__.py
├── tools/                  # Ferramentas que USAM a biblioteca
│   ├── run_pipeline.py     # (antigo autosinapi_pipeline.py, agora mais simples)
│   └── ...
├── tests/                  # Diretório para testes unitários
├── pyproject.toml
├── setup.py
└── README.md

```

#### **Fase 2: Criação e desenvolvimento dos testes unitários**

Aqui está um planejamento completo para a criação e desenvolvimento dos testes unitários para o módulo AutoSINAPI. Este plano servirá como uma diretriz para o desenvolvedor do módulo, garantindo que o toolkit que receberemos seja de alta qualidade.

A Filosofia: Por que Testar?
Antes de detalhar o plano, é crucial entender o valor que os testes trarão:

Garantia de Qualidade: Encontrar e corrigir bugs antes que eles cheguem ao nosso ambiente de produção.

Segurança para Refatorar: Permitir que o módulo AutoSINAPI evolua e seja otimizado no futuro. Se as mudanças não quebrarem os testes existentes, temos alta confiança de que o sistema continua funcionando.

Documentação Viva: Os testes são a melhor forma de documentar como uma função ou classe deve se comportar em diferentes cenários.

Design de Código Melhor: Escrever código testável naturalmente nos força a criar componentes menores, desacoplados e com responsabilidades claras.

Ferramentas Recomendadas
O ecossistema Python tem ferramentas padrão e excelentes para testes.

Framework de Teste: pytest - É o padrão da indústria. Simples de usar, poderoso e com um ecossistema de plugins fantástico.

Simulação (Mocking): pytest-mock - Essencial. Os testes unitários devem ser rápidos e isolados. Isso significa que não podemos fazer chamadas reais à internet (site da Caixa) ou a um banco de dados real durante os testes. Usaremos "mocks" para simular o comportamento desses sistemas externos.

Cobertura de Teste: pytest-cov - Mede qual porcentagem do nosso código está sendo executada pelos testes. Isso nos ajuda a identificar partes críticas que não foram testadas.

O Plano de Testes Unitários por Módulo
A estratégia de testes seguirá a mesma estrutura modular que definimos para a refatoração do AutoSINAPI.

Estrutura de Diretórios de Teste
/AutoSINAPI/
├── autosinapi/
│   ├── core/
│   │   ├── downloader.py
│   │   └── ...
│   └── ...
├── tests/                  # <--- Novo diretório para todos os testes
│   ├── core/
│   │   ├── test_downloader.py
│   │   ├── test_processor.py
│   │   └── test_database.py
│   ├── test_pipeline.py
│   └── fixtures/           # <--- Para guardar arquivos de teste (ex: um .xlsx pequeno)

## Plano de Testes Unitários e de Integração

A seguir, detalhamos o plano de testes para cada módulo do AutoSINAPI, utilizando boas práticas de Markdown para facilitar a leitura e consulta.

---

### 1. Testes para `core/downloader.py`

**Objetivo:**  
Garantir que a lógica de download, retry e tratamento de erros de rede funcione corretamente, sem chamadas reais à internet.

**Mock:**  
- `requests.get`

**Cenários de Teste:**

| Teste                        | Descrição                                                                                  |
|------------------------------|-------------------------------------------------------------------------------------------|
| `test_download_sucesso`      | Simula um `requests.get` que retorna status 200 OK e conteúdo de zip falso. Verifica se a função retorna o conteúdo esperado. |
| `test_download_falha_404`    | Simula um `requests.get` que levanta `HTTPError` 404. Verifica se o downloader trata o erro corretamente, levantando `DownloadError`. |
| `test_download_com_retry`    | Simula falha nas duas primeiras chamadas (ex: Timeout) e sucesso na terceira. Verifica se a lógica de retry é acionada. |
| `test_download_com_proxy`    | Verifica se, ao usar proxies, a chamada a `requests.get` é feita com o parâmetro `proxies` corretamente preenchido. |

---

### 2. Testes para `core/processor.py`

**Objetivo:**  
Garantir que o processamento dos dados do Excel (limpeza, normalização, transformação) está correto para diferentes cenários.

**Mocks/Dados de Teste:**  
- Pequenos DataFrames pandas ou arquivos `.xlsx` de exemplo em `tests/fixtures/`.

**Cenários de Teste:**

| Teste                        | Descrição                                                                                  |
|------------------------------|-------------------------------------------------------------------------------------------|
| `test_normalizacao_texto`    | Testa normalização de texto com acentos, maiúsculas/minúsculas e espaços extras.          |
| `test_limpeza_dataframe`     | Passa DataFrame com valores nulos, colunas "sujas" e tipos incorretos. Verifica limpeza e padronização. |
| `test_processamento_melt`    | Testa transformação "melt" em DataFrame de exemplo, verificando estrutura de colunas e linhas. |
| `test_identificacao_tipo_planilha` | Passa diferentes nomes de planilhas e verifica se retorna a configuração correta de `header_id` e `split_id`. |

---

### 3. Testes para `core/database.py`

**Objetivo:**  
Garantir que a lógica de interação com o banco de dados (criação de tabelas, inserção, deleção) gera os comandos SQL corretos, sem conexão real.

**Mock:**  
- Objeto `engine` do SQLAlchemy e suas conexões.

**Cenários de Teste:**

| Teste                        | Descrição                                                                                  |
|------------------------------|-------------------------------------------------------------------------------------------|
| `test_create_table_com_inferencia` | Passa DataFrame e verifica se o comando `CREATE TABLE` gerado contém nomes de coluna e tipos SQL corretos. |
| `test_insert_data_em_lotes`  | Passa DataFrame com mais de 1000 linhas e verifica se a inserção é chamada em múltiplos lotes. |
| `test_logica_de_duplicatas_substituir` | Simula registros existentes e política "substituir". Verifica se `DELETE FROM ...` é executado antes do `INSERT`. |
| `test_logica_de_duplicatas_agregar` | Simula política "agregar". Verifica se apenas dados não existentes são inseridos. |

---

### 4. Testes de Integração para `pipeline.py` e Interface Pública

**Objetivo:**  
Garantir que a função principal `run_etl` orquestra corretamente as chamadas aos componentes.

**Mock:**  
- Classes `Downloader`, `Processor` e `DatabaseManager`.

**Cenários de Teste:**

| Teste                        | Descrição                                                                                  |
|------------------------------|-------------------------------------------------------------------------------------------|
| `test_run_etl_fluxo_ideal`   | Simula funcionamento perfeito dos componentes. Verifica ordem das chamadas: `download()`, `process()`, `insert()`. |
| `test_run_etl_com_falha_no_download` | Simula exceção em `downloader.download()`. Verifica se `processor` e `database` não são chamados. |
| `test_run_etl_passa_configs_corretamente` | Chama `run_etl()` com configs específicas. Verifica se componentes mockados recebem os dicionários corretos. |

---

## 6. Atualização e Correção dos Testes (Setembro 2025)

**Objetivo:** Atualizar a suíte de testes para refletir a nova arquitetura do pipeline AutoSINAPI, garantindo que todos os testes passem e que a cobertura do código seja mantida ou ampliada.

### Situação Atual dos Testes

Após uma refatoração significativa do pipeline de ETL, a suíte de testes encontra-se parcialmente quebrada. Os principais problemas são:

-   **`tests/test_file_input.py` e `tests/test_pipeline.py`**: Falham devido à remoção da função `run_etl` e a mudanças na lógica interna do pipeline. As chamadas diretas à função foram substituídas por uma classe `Pipeline`, e os testes precisam ser adaptados para instanciar e mockar essa classe corretamente.
-   **`tests/core/test_database.py`**: Apresenta falhas relacionadas a mudanças na assinatura de métodos (ex: `save_data` agora exige um parâmetro `policy`) e a mensagens de erro que foram atualizadas.
-   **`tests/core/test_processor.py`**: Contém falhas devido à remoção de métodos privados que eram testados diretamente e a mudanças na assinatura de métodos públicos como `process_composicao_itens`.

### Situação Desejada

-   **Todos os testes passando**: A suíte de testes deve ser executada sem falhas.
-   **Cobertura de código**: A cobertura de testes deve ser mantida ou ampliada para abranger a nova arquitetura.
-   **Manutenibilidade**: Os testes devem ser fáceis de entender e manter.

### Plano de Ação Detalhado

| Arquivo | Ação Corretiva |
| --- | --- |
| **`tests/core/test_database.py`** | **1. Corrigir `test_save_data_failure`**: Atualizar a mensagem de erro esperada no `pytest.raises` para refletir a nova mensagem da exceção `DatabaseError`.<br>**2. Corrigir `test_save_data_success` e `test_save_data_failure`**: Adicionar o argumento `policy` na chamada do método `save_data`. |
| **`tests/core/test_processor.py`** | **1. Corrigir `test_process_composicao_itens`**: Ajustar a forma como o arquivo Excel de teste é criado, garantindo que o cabeçalho e o nome da planilha (`Analítico`) estejam corretos para que o `Processor` possa lê-lo. |
| **`tests/test_pipeline.py`** | **1. Ajustar `mock_pipeline` fixture**: <br> - Modificar o mock de `process_composicao_itens` para que o `parent_composicoes_details` retornado contenha a coluna `codigo`. <br> - Garantir que o mock de `_unzip_file` retorne um caminho que contenha um arquivo "Referência" simulado. <br> **2. Atualizar `caplog`**: Corrigir as mensagens de erro esperadas nas asserções dos testes de falha (`test_run_etl_download_error`, `test_run_etl_processing_error`, `test_run_etl_database_error`). |
| **`tests/test_file_input.py`** | **1. Ajustar `mock_pipeline` fixture**: Aplicar as mesmas correções do `test_pipeline.py` para garantir a consistência dos mocks. <br> **2. Corrigir `test_direct_file_input`**: Garantir que o método `save_data` seja chamado, corrigindo o `KeyError` no pipeline. <br> **3. Atualizar `caplog`**: Corrigir a mensagem de erro esperada no teste `test_invalid_input_file`. |

---

## Plano de Trabalho Sugerido

### 1. Configuração do Ambiente de Teste

- Criar a estrutura de diretórios `tests/`.
- Adicionar `pytest`, `pytest-mock` e `pytest-cov` ao `requirements.txt` de desenvolvimento.

### 2. Desenvolvimento Orientado a Testes (TDD)

1. **Começar pelos módulos mais isolados:**  
  - `processor.py` e `file_manager.py`: Escrever testes primeiro, implementar lógica para fazê-los passar.
2. **Testar `downloader.py`:**  
  - Foco na simulação das chamadas de rede.
3. **Testar `database.py`:**  
  - Simular conexão e verificar queries SQL geradas.
4. **Testes de integração:**  
  - Para `pipeline.py` e função pública `run_etl`, simulando as classes já testadas.

### 3. Integração Contínua (CI)

- Configurar ferramenta como **GitHub Actions** para rodar todos os testes automaticamente a cada push ou pull request.

### 4. Documentação dos Testes

Para garantir manutenibilidade e compreensão, documente cada teste com:

- **Descrição dos Testes:** Breve explicação do objetivo e comportamento esperado.
- **Pré-condições:** Estado necessário antes do teste (ex: banco de dados, arquivos de entrada).
- **Passos para Reproduzir:** Instruções detalhadas de execução, comandos e configurações.
- **Resultados Esperados:** Saídas e efeitos colaterais esperados.
- **Notas sobre Implementação:** Informações adicionais relevantes para entendimento ou manutenção.

> **Dica:** Mantenha a documentação dos testes sempre atualizada conforme novas funcionalidades forem adicionadas ao sistema.

---
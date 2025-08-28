
Como arquiteto da API, meu papel é definir o "contrato": como a API irá interagir com o módulo `AutoSINAPI`, quais as responsabilidades de cada um, e qual a arquitetura que garante a viabilidade econômica, segurança e performance que buscamos.

Vamos organizar essa ideia em um plano de desenvolvimento lógico e estruturado.

-----

### **Visão Geral da Arquitetura: API, Toolkit e Tarefas em Segundo Plano**

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

### **Parte 1: O Contrato de Serviço (As Diretrizes para o Módulo `AutoSINAPI`)**

Para que a API possa usar o `AutoSINAPI` como um toolkit, o módulo precisa evoluir para uma biblioteca que exponha uma interface (API) clara. Como desenvolvedor da API, eu defino o que preciso que essa biblioteca me entregue.

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

### **Parte 2: O Caminho do Desenvolvimento (Etapas Lógicas)**

Este é o roadmap que seguiremos.

#### **Fase 1: Evolução do `AutoSINAPI` para um Toolkit (Responsabilidade do Desenvolvedor do Módulo)**

Esta fase é sobre preparar o módulo para ser consumido pela nossa API.

  * **Etapa 1.1: Refatoração Estrutural:** Quebrar o `sinapi_utils.py` em módulos menores (`downloader.py`, `processor.py`, `database.py`) dentro de uma estrutura de pacote Python, como planejamos anteriormente.
  * **Etapa 1.2: Implementar a Lógica de Configuração Centralizada:** Remover toda a leitura de arquivos de configuração de dentro das classes e fazer com que elas recebam suas configurações via construtor (`__init__`).
  * **Etapa 1.3: Criar a Interface Pública:** Criar a função `run_etl(db_config, sinapi_config, mode)` que orquestra as chamadas para as classes internas.

    * **Etapa1.3.1: Desacoplar as Classes (Injeção de Dependência):** Em vez de uma classe criar outra (ex: self.downloader = SinapiDownloader()), ela deve recebê-la como um parâmetro em seu construtor (__init__(self, downloader)). Isso torna o código muito mais flexível e testável.
  * **Etapa 1.4: Implementar o Modo Duplo:** Dentro das classes `downloader` e `processor`, adicionar a lógica `if mode == 'server': ... else: ...` para lidar com operações em memória vs. em disco.
  * **Etapa 1.5: Empacotamento:** Garantir que o módulo seja instalável via `pip` com um `setup.py` ou `pyproject.toml`.

Nova Estrutura de Diretórios revista:

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
└── ...
1. Testes para core/downloader.py
Objetivo: Garantir que a lógica de download, retry e tratamento de erros de rede funcione corretamente, sem fazer nenhuma chamada real à internet.

O que Simular (Mock): A função requests.get.

Cenários de Teste:

test_download_sucesso: Simular um requests.get que retorna um status 200 OK e um conteúdo de zip falso. Verificar se a função retorna o conteúdo esperado.

test_download_falha_404: Simular um requests.get que levanta um erro HTTPError com status 404. Verificar se o downloader trata o erro corretamente, talvez levantando uma exceção customizada DownloadError.

test_download_com_retry: Simular um requests.get que falha nas duas primeiras chamadas (ex: com Timeout) e funciona na terceira. Verificar se a lógica de retry é acionada.

test_download_com_proxy: Verificar se, ao usar proxies, a chamada a requests.get é feita com o parâmetro proxies preenchido corretamente.

2. Testes para core/processor.py
Objetivo: Garantir que o processamento dos dados do Excel (limpeza, normalização, transformação) está correto para diferentes cenários.

O que Simular (Mock): Não há mocks externos, mas usaremos dados de teste. Criaremos pequenos DataFrames pandas ou até mesmo um arquivo .xlsx de exemplo no diretório tests/fixtures/ com dados "sujos".

Cenários de Teste:

test_normalizacao_texto: Testar a função de normalizar texto com strings contendo acentos, maiúsculas/minúsculas e espaços extras. Verificar se a saída está correta.

test_limpeza_dataframe: Passar um DataFrame com valores nulos, colunas com nomes "sujos" e tipos de dados incorretos. Verificar se o DataFrame de saída está limpo e padronizado.

test_processamento_melt: Para as planilhas que precisam de "unpivot", passar um DataFrame de exemplo e verificar se a transformação melt resulta na estrutura de colunas e linhas esperada.

test_identificacao_tipo_planilha: Passar diferentes nomes de planilhas (ex: "SINAPI_CSD_...", "SINAPI_ISD_...") e verificar se a função retorna a configuração correta de header_id e split_id.

3. Testes para core/database.py
Objetivo: Garantir que a lógica de interação com o banco de dados (criação de tabelas, inserção, deleção) gera os comandos SQL corretos, sem conectar a um banco de dados real.

O que Simular (Mock): O objeto engine do SQLAlchemy e suas conexões. Vamos verificar quais comandos são enviados para o método .execute().

Cenários de Teste:

test_create_table_com_inferencia: Passar um DataFrame e verificar se o comando CREATE TABLE ... gerado contém os nomes de coluna e os tipos SQL corretos.

test_insert_data_em_lotes: Passar um DataFrame com mais de 1000 linhas (o tamanho do lote) e verificar se o método de inserção é chamado múltiplas vezes (uma para cada lote).

test_logica_de_duplicatas_substituir: Simular que o banco já contém alguns registros. Chamar a função de validação com a política "substituir". Verificar se um comando DELETE FROM ... é executado antes do INSERT.

test_logica_de_duplicatas_agregar: Fazer o mesmo que o anterior, mas com a política "agregar". Verificar se os dados inseridos são apenas os que não existiam no banco.

4. Testes de Integração para pipeline.py e a Interface Pública
Objetivo: Garantir que a função principal run_etl orquestra as chamadas aos outros componentes na ordem correta.

O que Simular (Mock): As classes Downloader, Processor e DatabaseManager inteiras.

Cenários de Teste:

test_run_etl_fluxo_ideal: Simular que cada componente funciona perfeitamente. Chamar run_etl(). Verificar se downloader.download() foi chamado, depois processor.process(), e por último database.insert().

test_run_etl_com_falha_no_download: Simular que o downloader.download() levanta uma exceção. Chamar run_etl(). Verificar se o processor e o database não foram chamados, provando que o pipeline parou corretamente.

test_run_etl_passa_configs_corretamente: Chamar run_etl() com dicionários de configuração específicos. Verificar se os construtores ou métodos dos componentes mockados foram chamados com esses mesmos dicionários.

Plano de Trabalho Sugerido
Configurar o Ambiente de Teste:

Criar a estrutura de diretórios tests/.

Adicionar pytest, pytest-mock e pytest-cov ao requirements.txt de desenvolvimento.

Desenvolvimento Orientado a Testes (por módulo):

Começar pelos módulos mais isolados e com menos dependências (ex: processor.py e file_manager.py). Escrever os testes primeiro, vê-los falhar, e depois implementar a lógica no módulo para fazê-los passar.

Em seguida, testar o downloader.py, focando na simulação das chamadas de rede.

Depois, o database.py, focando na simulação da conexão e na verificação das queries SQL geradas.

Por último, escrever os testes de integração para o pipeline.py e a função pública run_etl, simulando as classes já testadas.

Integração Contínua (CI):

Após a criação dos testes, o passo final é configurar uma ferramenta como GitHub Actions para rodar todos os testes automaticamente a cada push ou pull request. Isso garante que nenhum código novo quebre a funcionalidade existente.

Documentação dos Testes:

Para garantir a manutenibilidade e a compreensão do código, é essencial documentar os testes de forma clara e concisa. A documentação deve incluir:

1. **Descrição dos Testes**: Para cada teste, incluir uma breve descrição do que está sendo testado e qual é o comportamento esperado.

2. **Pré-condições**: Listar quaisquer pré-condições que devem ser atendidas antes da execução do teste (ex: estado do banco de dados, arquivos de entrada, etc.).

3. **Passos para Reproduzir**: Instruções detalhadas sobre como executar o teste, incluindo comandos específicos e configurações necessárias.

4. **Resultados Esperados**: Descrever o que constitui um resultado bem-sucedido para o teste, incluindo saídas esperadas e efeitos colaterais.

5. **Notas sobre Implementação**: Qualquer informação adicional que possa ser útil para entender a lógica do teste ou sua implementação.

Essa documentação deve ser mantida atualizada à medida que os testes evoluem e novas funcionalidades são adicionadas ao sistema.

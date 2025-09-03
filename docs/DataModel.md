# **Modelo de Dados e ETL para o Módulo SINAPI**

## 1\. Introdução

### 1.1. Objetivo

Este documento detalha a arquitetura de dados e o processo de **ETL (Extração, Transformação e Carga)** para a criação de um módulo Python OpenSource. O objetivo é processar os arquivos mensais do **SINAPI**, consolidando os dados em um banco de dados **PostgreSQL** de forma robusta, normalizada e com total rastreabilidade histórica.

A estrutura resultante permitirá que a comunidade de engenharia e arquitetura realize consultas complexas para orçamentação, planejamento e análise histórica, seja através de uma `API` ou acessando o banco de dados localmente.

### 1.2. Visão Geral das Fontes de Dados

O ecossistema de dados do SINAPI é composto por dois arquivos principais, que devem ser processados em conjunto para garantir a consistência e a integridade do banco de dados:

1.  **`SINAPI_Referência_AAAA_MM.xlsx`**: Arquivo principal contendo os catálogos de preços, custos e a estrutura analítica das composições para o mês de referência.
2.  **`SINAPI_manutencoes_AAAA_MM.xlsx`**: Arquivo de suporte que detalha todo o histórico de alterações (ativações, desativações, mudanças de descrição) dos insumos e composições. É a fonte da verdade para o ciclo de vida de cada item.

## 2\. Modelo de Dados Relacional (PostgreSQL)

O modelo é projetado para máxima integridade, performance e clareza, separando entidades de catálogo, dados de série histórica, suas relações estruturais e o histórico de eventos.

### 2.1. Catálogo (Entidades Principais)

Armazenam a descrição única e o **estado atual** de cada insumo e composição.

#### Tabela `insumos`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `codigo` | `INTEGER` | **Chave Primária (PK)** |
| `descricao` | `TEXT` | Descrição completa do insumo. |
| `unidade` | `VARCHAR` | Unidade de medida (UN, M2, M3, KG). |
| `classificacao` | `TEXT` | Classificação hierárquica do insumo. |
| `status` | `VARCHAR` | `'ATIVO'` ou `'DESATIVADO'`. **Controlado pelo ETL de manutenções**. |

#### Tabela `composicoes`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `codigo` | `INTEGER` | **Chave Primária (PK)** |
| `descricao` | `TEXT` | Descrição completa da composição. |
| `unidade` | `VARCHAR` | Unidade de medida (UN, M2, M3). |
| `grupo` | `VARCHAR` | Grupo ao qual a composição pertence. |
| `status` | `VARCHAR` | `'ATIVO'` ou `'DESATIVADO'`. **Controlado pelo ETL de manutenções**. |

### 2.2. Dados Mensais (Série Histórica)

Recebem novos registros a cada mês, construindo o histórico de preços e custos.

#### Tabela `precos_insumos_mensal`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `insumo_codigo` | `INTEGER` | `FK` -\> `insumos.codigo` |
| `uf` | `CHAR(2)` | Unidade Federativa. |
| `data_referencia` | `DATE` | Primeiro dia do mês de referência. |
| `preco_mediano` | `NUMERIC` | Preço do insumo na UF/Data/Regime. |
| `regime` | `VARCHAR` | `'NAO_DESONERADO'`, `'DESONERADO'`, `'SEM_ENCARGOS'`. |
| **PK Composta** | | (`insumo_codigo`, `uf`, `data_referencia`, `regime`) |

#### Tabela `custos_composicoes_mensal`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `composicao_codigo`| `INTEGER` | `FK` -\> `composicoes.codigo` |
| `uf` | `CHAR(2)` | Unidade Federativa. |
| `data_referencia` | `DATE` | Primeiro dia do mês de referência. |
| `custo_total` | `NUMERIC` | Custo da composição na UF/Data/Regime. |
| `regime` | `VARCHAR` | `'NAO_DESONERADO'`, `'DESONERADO'`, `'SEM_ENCARGOS'`. |
| **PK Composta** | | (`composicao_codigo`, `uf`, `data_referencia`, `regime`) |

### 2.3. Estrutura das Composições (Relacionamentos)

Modelam a estrutura hierárquica das composições. Devem ser totalmente recarregadas a cada mês para refletir a estrutura mais atual.

#### Tabela `composicao_insumos`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `composicao_pai_codigo` | `INTEGER` | `FK` -\> `composicoes.codigo` |
| `insumo_filho_codigo` | `INTEGER` | `FK` -\> `insumos.codigo` |
| `coeficiente` | `NUMERIC` | Coeficiente de consumo do insumo. |
| **PK Composta** | | (`composicao_pai_codigo`, `insumo_filho_codigo`) |

#### Tabela `composicao_subcomposicoes`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `composicao_pai_codigo` | `INTEGER` | `FK` -\> `composicoes.codigo` |
| `composicao_filho_codigo` | `INTEGER` | `FK` -\> `composicoes.codigo` |
| `coeficiente` | `NUMERIC` | Coeficiente de consumo da subcomposição. |
| **PK Composta** | | (`composicao_pai_codigo`, `composicao_filho_codigo`) |

### 2.4. Histórico de Manutenções

Esta tabela é o **log imutável** de todas as mudanças ocorridas nos itens do SINAPI.

#### Tabela `manutencoes_historico`

| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `item_codigo` | `INTEGER` | Código do Insumo ou Composição. |
| `tipo_item` | `VARCHAR` | `'INSUMO'` ou `'COMPOSICAO'`. |
| `data_referencia` | `DATE` | Data do evento de manutenção (primeiro dia do mês). |
| `tipo_manutencao` | `TEXT` | Descrição da manutenção realizada (Ex: 'DESATIVAÇÃO'). |
| `descricao_item` | `TEXT` | Descrição do item no momento do evento. |
| **PK Composta** | | (`item_codigo`, `tipo_item`, `data_referencia`, `tipo_manutencao`) |

### 2.5. Visão Unificada (View) para Simplificar Consultas

Para facilitar a consulta de todos os itens de uma composição (sejam insumos ou subcomposições) sem a necessidade de acessar duas tabelas, uma `VIEW` deve ser criada no banco de dados.

#### `vw_composicao_itens_unificados`

```sql
CREATE OR REPLACE VIEW vw_composicao_itens_unificados AS
SELECT
    composicao_pai_codigo,
    insumo_filho_codigo AS item_codigo,
    'INSUMO' AS tipo_item,
    coeficiente
FROM
    composicao_insumos
UNION ALL
SELECT
    composicao_pai_codigo,
    composicao_filho_codigo AS item_codigo,
    'COMPOSICAO' AS tipo_item,
    coeficiente
FROM
    composicao_subcomposicoes;
```

-----

## 3\. Processo de ETL (Fluxo de Execução Detalhado)

O fluxo de execução foi projetado para adotar uma abordagem **"Manutenções Primeiro"**, garantindo a máxima consistência dos dados.

### 3.1. Parâmetros de Entrada

  * **Caminho do Arquivo de Referência:** `path/to/SINAPI_Referência_AAAA_MM.xlsx`
  * **Caminho do Arquivo de Manutenções:** `path/to/SINAPI_manutencoes_AAAA_MM.xlsx`
  * **Data de Referência:** Derivada do nome do arquivo (ex: `2025-07-01`).
  * **String de Conexão com o Banco de Dados.**

### **FASE 1: Processamento do Histórico de Manutenções**

Esta fase estabelece a fonte da verdade sobre o status de cada item.

1.  **Extração:**

      * Carregar a planilha `Manutenções` do arquivo `SINAPI_manutencoes_AAAA_MM.xlsx`.
      * **Atenção:** O cabeçalho está na linha 6, portanto, use `header=5` na leitura.

2.  **Transformação:**

      * Renomear as colunas para o padrão do banco de dados (ex: `Código` -\> `item_codigo`).
      * Converter a coluna `Referência` (formato `MM/AAAA`) para um `DATE` válido (primeiro dia do mês, ex: `07/2025` -\> `2025-07-01`).
      * Limpar e padronizar os dados textuais.

3.  **Carga:**

      * Inserir os dados transformados na tabela `manutencoes_historico`.
      * Utilizar uma cláusula `ON CONFLICT DO NOTHING` na chave primária composta para evitar a duplicação de registros históricos caso o ETL seja re-executado.

### **FASE 2: Sincronização de Status dos Catálogos**

Esta fase utiliza os dados carregados na Fase 1 para atualizar o estado atual dos itens.

1.  **Lógica de Atualização:** Executar um script (em Python/SQL) que:
      * Para cada item (`código`, `tipo`) presente na tabela `manutencoes_historico`, identifique a **manutenção mais recente** (última `data_referencia`).
      * Verifique se o `tipo_manutencao` dessa última entrada indica uma desativação (ex: `tipo_manutencao ILIKE '%DESATIVAÇÃO%'`).
      * Se for uma desativação, executar um `UPDATE` na tabela correspondente (`insumos` ou `composicoes`), ajustando o campo `status` para `'DESATIVADO'`.

### **FASE 3: Processamento dos Dados de Referência (Preços, Custos e Estrutura)**

Esta fase processa o arquivo principal do SINAPI, operando sobre catálogos cujo status já foi sincronizado.

1.  **Extração:**

      * Carregar as planilhas de referência (`ISD`, `ICD`, `ISE`, `CSD`, `CCD`, `CSE`, `Analítico`) do arquivo `SINAPI_Referência_AAAA_MM.xlsx`.
      * **Atenção:** O cabeçalho dos dados começa na linha 9, portanto, use `header=9`.

2.  **Transformação:**

      * **Enriquecimento de Contexto (Regime):** Adicionar uma coluna `regime` a cada DataFrame de preço/custo, mapeando o nome da planilha para o valor (`'NAO_DESONERADO'`, `'DESONERADO'`, `'SEM_ENCARGOS'`).
      * **Unpivot (Melt):** Transformar os DataFrames do formato "largo" (UFs em colunas) para o formato "longo" (UFs em linhas).
      * **Consolidação:** Unir os DataFrames de mesmo tipo (insumos com insumos, composições com composições).
      * **Separação dos Dados:** A partir dos DataFrames consolidados, criar os DataFrames finais para cada tabela de destino (`df_catalogo_insumos`, `df_precos_mensal`, etc.).

3.  **Carga (Ordem Crítica):**

    1.  **Carregar Catálogos (UPSERT):**
          * Carregar `df_catalogo_insumos` na tabela `insumos` e `df_catalogo_composicoes` em `composicoes`.
          * **Lógica:** Usar `ON CONFLICT (codigo) DO UPDATE SET descricao = EXCLUDED.descricao, ...`.
          * **Importante:** Não atualizar a coluna `status` nesta etapa. Novos itens serão inseridos com o `status` default (`'ATIVO'`).
    2.  **Recarregar Estrutura (TRUNCATE/INSERT):**
          * Executar `TRUNCATE TABLE composicao_insumos, composicao_subcomposicoes;`.
          * Inserir os novos DataFrames de estrutura.
            .
    3.  **Carregar Dados Mensais (INSERT):**
          * Inserir os DataFrames de preços e custos em suas respectivas tabelas. Utilizar `ON CONFLICT DO NOTHING` para segurança em re-execuções.

## 4\. Diretrizes para API e Consultas

O modelo de dados robusto criado pelo `autoSINAPI` serve como uma base poderosa tanto para o uso programático (toolkit) quanto para a criação de APIs RESTful performáticas. Esta seção descreve a interface principal do toolkit e exemplifica endpoints que podem ser construídos sobre os dados processados.

### 4.1. Interface Programática (Toolkit)

A função `run_etl` é a interface pública principal para executar o pipeline de forma programática.

| Parâmetro | Tipo | Descrição | Padrão |
| :--- | :--- | :--- | :--- |
| **`db_config`** | `Dict` | Dicionário com as credenciais de conexão do PostgreSQL. | *Obrigatório* |
| **`sinapi_config`** | `Dict` | Dicionário com as configurações de referência dos dados SINAPI. | *Obrigatório* |
| **`mode`** | `str` | Modo de operação: `'local'` (baixa os arquivos) ou `'server'` (usa arquivos locais). | `'local'` |
| **`log_level`** | `str` | Nível de detalhe dos logs (`'INFO'`, `'DEBUG'`, etc.). | `'INFO'` |

-----

#### **Estrutura do `db_config`**

```python
{
    "host": "seu_host_db",      # Ex: "localhost" ou "db" (para Docker)
    "port": 5432,               # Porta do PostgreSQL
    "database": "seu_db_name",  # Nome do banco de dados
    "user": "seu_usuario",      # Usuário do banco de dados
    "password": "sua_senha"     # Senha do usuário
}
```

*Todos os campos são obrigatórios.*

-----

#### **Estrutura do `sinapi_config`**

```python
{
    "year": 2023,                 # Ano de referência (obrigatório)
    "month": 7,                   # Mês de referência (obrigatório)
    "type": "REFERENCIA",         # Tipo de caderno ("REFERENCIA" ou "DESONERADO")
    "duplicate_policy": "substituir" # Política de duplicatas ("substituir" ou "append")
}
```

*`year` e `month` são obrigatórios. Os demais possuem valores padrão.*

-----

### 4.2. Exemplos de Casos de Uso (API REST)

A estrutura do banco de dados permite a criação de endpoints de API poderosos para consultar os dados de forma eficiente.

#### **Exemplo 1: Obter o custo de uma composição**

| | |
| :--- | :--- |
| **Endpoint** | `GET /custo_composicao` |
| **Parâmetros** | `codigo`, `uf`, `data_referencia`, `regime` |
| **Lógica** | Busca direta na tabela `custos_composicoes_mensal`, com um `JOIN` opcional na tabela `composicoes` para verificar o `status` do item (ativo/inativo). |

\<br\>

#### **Exemplo 2: Explodir a estrutura completa de uma composição**

| | |
| :--- | :--- |
| **Endpoint** | `GET /composicao/{codigo}/estrutura` |
| **Lógica** | Utiliza a view `vw_composicao_itens_unificados` para montar a árvore completa de insumos e subcomposições de um item. Uma consulta recursiva (CTE) é ideal para esta finalidade. |

\<br\>

#### **Exemplo 3: Rastrear o histórico de um insumo**

| | |
| :--- | :--- |
| **Endpoint** | `GET /insumo/{codigo}/historico` |
| **Lógica** | Consulta direta na tabela `manutencoes_historico` para retornar todas as manutenções (inclusão, exclusão, alteração) de um insumo específico, ordenadas por data. |
| **Exemplo SQL** | `sql<br>SELECT * FROM manutencoes_historico<br>WHERE item_codigo = :codigo AND tipo_item = 'INSUMO'<br>ORDER BY data_referencia DESC;<br>` |

## 5. Conclusão

A adoção desta arquitetura de dados e fluxo de ETL resultará em um sistema:

*   **Robusto**: Capaz de lidar com a evolução dos dados do SINAPI ao longo do tempo.
*   **Rastreável**: Mantém um histórico completo das alterações, fundamental para auditoria e análise comparativa.
*   **Performático**: O modelo normalizado permite consultas rápidas e eficientes.
*   **Flexível**: A estrutura suporta uma ampla gama de consultas, desde simples buscas de preço até análises complexas de planejamento.
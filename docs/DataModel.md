# Estrutura de Dados e ETL para Módulo Python SINAPI

## 1. Introdução

### 1.1. Objetivo do Documento
Este documento detalha a arquitetura de dados e o processo de **ETL (Extração, Transformação e Carga)** recomendados para a criação de um módulo Python OpenSource. O objetivo do módulo é processar as planilhas mensais do **SINAPI** e consolidar os dados em um banco de dados **PostgreSQL**, permitindo que profissionais de engenharia e arquitetura realizem consultas complexas para orçamentação e planejamento de obras via `API` ou localmente.

### 1.2. Visão Geral do Ecossistema SINAPI
Os dados do SINAPI são distribuídos em múltiplas planilhas que representam diferentes facetas do sistema de custos:

*   **Catálogos**: Listas de Insumos e Composições.
*   **Estruturas**: A relação de dependência entre composições e seus itens.
*   **Preços e Custos**: Valores monetários regionalizados (por UF) e sensíveis à política de desoneração.
*   **Metadados**: Informações auxiliares como "Famílias de Insumos" e o histórico de manutenções (ativações, desativações, etc.).

A arquitetura proposta visa modelar essas facetas de forma coesa e histórica.

## 2. Modelo de Dados Relacional (PostgreSQL)

A estrutura é organizada em tabelas de **Catálogo**, **Dados Mensais** e **Suporte/Histórico**.

### 2.1. Tabelas de Catálogo (Entidades Principais)
Estas tabelas contêm a descrição dos objetos centrais, que mudam com pouca frequência.

#### Tabela `insumos`
| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `codigo` | `INTEGER` | **Chave Primária** |
| `descricao` | `TEXT` | |
| `unidade` | `VARCHAR` | |
| `status` | `VARCHAR` | `Default: 'ATIVO'`. Controla o estado (`ATIVO`/`DESATIVADO`). |

#### Tabela `composicoes`
| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `codigo` | `INTEGER` | **Chave Primária** |
| `descricao` | `TEXT` | |
| `unidade` | `VARCHAR` | |
| `grupo` | `VARCHAR` | |
| `status` | `VARCHAR` | `Default: 'ATIVO'`. Controla o estado (`ATIVO`/`DESATIVADO`). |

### 2.2. Tabelas de Dados Mensais (Série Histórica)
Estas tabelas recebem novos registros a cada mês, construindo o histórico de preços e custos.

#### Tabela `precos_insumos_mensal`
| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `insumo_codigo` | `INTEGER` | `FK` -> `insumos.codigo` |
| `uf` | `CHAR(2)` | |
| `data_referencia` | `DATE` | |
| `preco_mediano` | `NUMERIC` | |
| `desonerado` | `BOOLEAN` | |
| **Chave Primária Composta** | | (`insumo_codigo`, `uf`, `data_referencia`, `desonerado`) |

#### Tabela `custos_composicoes_mensal`
| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `composicao_codigo` | `INTEGER` | `FK` -> `composicoes.codigo` |
| `uf` | `CHAR(2)` | |
| `data_referencia` | `DATE` | |
| `custo_total` | `NUMERIC` | |
| `percentual_mao_de_obra` | `NUMERIC` | |
| `desonerado` | `BOOLEAN` | |
| **Chave Primária Composta** | | (`composicao_codigo`, `uf`, `data_referencia`, `desonerado`) |

### 2.3. Tabelas de Suporte e Histórico
Estas tabelas modelam os relacionamentos e registram as mudanças ao longo do tempo.

#### Tabela `composicao_itens`
| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `composicao_pai_codigo` | `INTEGER` | `FK` -> `composicoes.codigo` |
| `item_codigo` | `INTEGER` | |
| `tipo_item` | `VARCHAR` | ('INSUMO' ou 'COMPOSICAO') |
| `coeficiente` | `NUMERIC` | |
| **Chave Primária Composta** | | (`composicao_pai_codigo`, `item_codigo`, `tipo_item`) |

#### Tabela `manutencoes_historico` (Tabela Chave para Gestão de Histórico)
| Coluna | Tipo | Restrições/Descrição |
| :--- | :--- | :--- |
| `item_codigo` | `INTEGER` | |
| `tipo_item` | `VARCHAR` | ('INSUMO' ou 'COMPOSICAO') |
| `data_referencia` | `DATE` | |
| `tipo_manutencao` | `VARCHAR` | Ex: 'DESATIVAÇÃO', 'ALTERACAO DE DESCRICAO' |
| `descricao_anterior` | `TEXT` | `Nullable` |
| `descricao_nova` | `TEXT` | `Nullable` |
| **Chave Primária Composta** | | (`item_codigo`, `tipo_item`, `data_referencia`, `tipo_manutencao`) |

## 3. Processo de ETL (Extract, Transform, Load)

O módulo Python deve implementar uma classe ou conjunto de funções que orquestre o seguinte fluxo mensal:

### 3.1. Etapa 1: Extração
Identificar e carregar em memória (usando `Pandas DataFrames`, por exemplo) todos os arquivos CSV relevantes do mês de referência (ex: `ISD.csv`, `CSD.csv`, `Analitico.csv`, `Manutencoes.csv`, etc.).

### 3.2. Etapa 2: Transformação (Regras de Negócio)

#### Processar Manutenções (Arquivo `Manutencoes.csv`)
Esta é a primeira e mais importante etapa da transformação.
1.  Para cada linha no arquivo de manutenções, criar um registro na tabela `manutencoes_historico`.
2.  Com base na manutenção mais recente de cada item, atualizar a coluna `status` nas tabelas `insumos` e `composicoes`. Por exemplo, se a última entrada para a composição `95995` foi 'DESATIVAÇÃO', o campo `composicoes.status` para esse código deve ser atualizado para `'DESATIVADO'`.
3.  Criar ou atualizar os registros nas tabelas de catálogo (`insumos`, `composicoes`). A lógica deve ser de **UPSERT**: se o código já existe, atualize a descrição e o status se necessário; se não existe, insira um novo registro.

#### Processar Catálogos e Estruturas (Arquivo `Analitico.csv`)
*   Popular a tabela `composicao_itens` com as relações de hierarquia. Esta tabela deve ser completamente recarregada a cada mês para refletir a estrutura mais atual das composições.

#### Processar Preços e Custos (Arquivos `ISD`, `CSD`, `Mão de Obra`, etc.)
1.  **Unpivot**: Transformar os dados dos arquivos de preço/custo, que têm UFs como colunas, para um formato de linhas (`item`, `uf`, `valor`).
2.  **Consolidar**: Unir os dados das planilhas "COM Desoneração" e "SEM Desoneração", adicionando a coluna booleana `desonerado`.
3.  **Enriquecer**: Adicionar a coluna `data_referencia` (ex: `'2025-07-01'`) a todos os registros.

### 3.3. Etapa 3: Carga
1.  Conectar-se ao banco de dados PostgreSQL.
2.  Executar as operações de carga na seguinte ordem:
    *   **UPSERT** nas tabelas de catálogo (`insumos`, `composicoes`).
    *   **INSERT** na tabela de histórico (`manutencoes_historico`), ignorando registros duplicados.
    *   **DELETE/INSERT** na tabela de estrutura (`composicao_itens`) para garantir que ela esteja sempre atualizada.
    *   **INSERT** nas tabelas de dados mensais (`precos_insumos_mensal`, `custos_composicoes_mensal`).

## 4. Diretrizes para a API e Consultas

Com os dados estruturados desta forma, a API pode fornecer endpoints poderosos e performáticos.

#### Exemplo de Endpoint para Orçamento: `GET /custo_composicao`
*   **Parâmetros**: `codigo`, `uf`, `data_referencia`, `desonerado`
*   **Lógica**: A consulta SQL simplesmente buscaria o registro correspondente na tabela `custos_composicoes_mensal`.

#### Exemplo de Endpoint para Planejamento: `GET /composicao/{codigo}/estrutura`
*   **Lógica**: Uma consulta SQL recursiva (`WITH RECURSIVE`) na tabela `composicao_itens` pode "explodir" toda a árvore de dependências de uma composição, listando todos os insumos de mão de obra e seus respectivos coeficientes, que são a base para o cálculo de produtividade e tempo de execução.

#### Exemplo de Endpoint para Histórico: `GET /insumo/{codigo}/historico`
*   **Lógica**: A consulta buscaria todos os registros na tabela `manutencoes_historico` para o código de insumo fornecido, permitindo rastrear todas as mudanças que ele sofreu.

## 5. Conclusão

A adoção desta arquitetura de dados e fluxo de ETL resultará em um sistema:

*   **Robusto**: Capaz de lidar com a evolução dos dados do SINAPI ao longo do tempo.
*   **Rastreável**: Mantém um histórico completo das alterações, fundamental para auditoria e análise comparativa.
*   **Performático**: O modelo normalizado permite consultas rápidas e eficientes.
*   **Flexível**: A estrutura suporta uma ampla gama de consultas, desde simples buscas de preço até análises complexas de planejamento.
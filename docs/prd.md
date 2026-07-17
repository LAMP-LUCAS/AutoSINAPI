# 📋 PRD — autoSINAPI ETL: Ingestão de Dados SINAPI

Este documento de Requisitos de Produto (PRD) estabelece as especificações de negócio para o processamento de planilhas e ingestão automática de dados do SINAPI.

---

## 1. Visão Geral e Objetivos

O módulo ETL é responsável por:
1.  **Download Automático:** Buscar os arquivos comprimidos (.zip) diretamente do site da Caixa Econômica Federal ou utilizar arquivos locais (Smart Discovery).
2.  **Pré-Processamento:** Converter planilhas Excel pesadas (`.xlsx`) em CSVs parciais otimizados.
3.  **Processamento e Normalização:** Analisar arquivos de referência de preços e composições, extraindo os itens cadastrados e gerando a árvore analítica.
4.  **Carga de Dados Relacional:** Inserir dados aplicando a lógica de transação atômica e UPSERTs rastreáveis para suportar retificações.

---

## 2. Fontes de Dados e Regras de Negócio

### 2.1. Arquivo de Referência (`SINAPI_Referência_AAAA_MM.xlsx`)
*   **Contém:** Insumos, composições e estruturas.
*   **Regra de Processamento:** As abas de insumos (`ISD`, `ISE`) e composições (`CSD`, `CCD`) devem ser processadas e normalizadas separando o preço mediano por Unidade Federativa (UF), Regime Tributário (`DESONERADO` ou `NAO_DESONERADO`) e data de referência.
*   **Importante:** A estrutura analítica de cada composição (quais insumos/subcomposições compõem o item pai) deve ser recarregada a cada execução mensal, pois o SINAPI altera composições frequentemente.

### 2.2. Arquivo de Manutenções (`SINAPI_manutencoes_AAAA_MM.xlsx`)
*   **Contém:** Log oficial de ativações, desativações e modificações de descrições feitas pela Caixa Econômica.
*   **Regra de Processamento:** O ETL deve ler a aba `Manutenções` (cabeçalho na linha 6) e atualizar o `status` (`ATIVO` / `DESATIVADO`) de insumos e composições nas tabelas do banco, respeitando o evento mais recente.

---

## 3. Requisitos de Confiabilidade e Rastreabilidade

*   **Rastreabilidade Atômica:** Todo registro nas tabelas do banco de dados deve possuir as colunas de auditoria:
    *   `created_at` / `updated_at`: timestamps de controle.
    *   `sinapi_versao`: versão do arquivo processado (ex: `"2025.09"`).
    *   `etl_run_id`: UUID gerado na execução para agrupar todas as alterações daquela execução.
*   **Tratamento de Itens Ausentes (Placeholders):** Em caso de composições que referenciem insumos inexistentes no catálogo de referência de preços (muito comum em dados brutos do SINAPI), o ETL deve criar automaticamente registros de placeholder na tabela `insumos` ou `composicoes` para evitar falha de chave estrangeira (Integridade Referencial).
*   **Política de Duplicação (UPSERT):** O ETL deve utilizar UPSERT (`ON CONFLICT DO UPDATE`) para permitir re-execuções sem duplicar dados, corrigindo retificações oficiais sem quebrar o histórico.

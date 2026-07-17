# 📦 autoSINAPI ETL — Documentação do Core

Bem-vindo à documentação do **autoSINAPI ETL**, a biblioteca Python open-source (GPLv3) responsável pelo motor de extração, transformação e carga dos dados oficiais do SINAPI (Caixa Econômica Federal) para banco de dados relacional.

---

## 🗺️ Mapeamento do Bounded Context (DDD)

Este módulo atua de forma isolada e autônoma, mapeando as planilhas brutas do SINAPI em estruturas relacionais normalizadas.

*   **Lógica Principal (Domain Services):** Extração de PDFs e planilhas XLSX, parseamento de tabelas de insumos e composições, ordenação de dependências e preservação de relacionamentos históricos.
*   **Portas de Entrada (Inbound Ports):** Função unificada `run_etl(...)` no ponto de entrada do pacote.
*   **Portas de Saída (Outbound Ports):** Conectores de banco de dados e expurgo de cache.

---

## 📂 Índice da Documentação (Template Unificado)

Para facilitar a navegação no core de processamento, siga os links locais dos arquivos estruturados:

1.  **[Requisitos do Produto (prd.md)](./prd.md):** Definição de escopo, regras de negócios da Caixa Econômica, arquivos de suporte e histórico de manutenções.
2.  **[Arquitetura do Módulo (architecture.md)](./architecture.md):** Visão detalhada de Domínio, Portas e Adaptadores (MVC-Hexagonal) da biblioteca de ETL.
3.  **[Dicionário de Dados (DataModel.md)](./DataModel.md):** Definição estrita das tabelas, chaves primárias e views de consolidação analítica.
4.  **[Decisões de Arquitetura (adrs/)](./adrs/):**
    *   **[ADR 001 — Bulk Insert via COPY](./adrs/001-copy-bulk.md):** Decisão sobre carregamento massivo de alta performance no Postgres.
    *   **[ADR 002 — Invalidação de Cache no Redis](./adrs/002-cache-invalidation.md):** Estratégia de expurgo pós-atualização.
4.  **[Agilidade e Histórico (agile/)](./agile/):**
    *   **[Sprint - Enriquecimento de Dados](./agile/SPRINT_ETL_ENRICHMENT.md)**
    *   **[Sprint - Hardening de SSOT](./agile/SPRINT_SSOT_HARDENING.md)**
5.  **[Loops de Qualidade (loops/)](./loops/):**
    *   **[Audit Report — Ingestão](./loops/AUDIT_REPORT.md):** Análise de gaps nas tabelas de referência e histórico.

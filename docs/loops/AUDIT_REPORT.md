# Loop de Auditoria (Audit Report) — autoSINAPI ETL

Este relatório de qualidade de engenharia identifica as falhas no processo atual de carga de dados e o plano de refatoração do ETL.

---

## 1. Gaps Identificados na Carga Atual

1.  **Inserções Bloqueantes e Lentas:**
    *   O processador de banco de dados (`database.py`) realiza inserções via SQLAlchemy / Pandas `to_sql`. Isso consome CPU excessiva e leva a timeouts frequentes durante o carregamento de grandes volumes de insumos.
2.  **Ignorância de Retificações do SINAPI:**
    *   Ao utilizar políticas simples de `DO NOTHING` no banco de dados, o ETL ignora quando a Caixa Econômica publica arquivos corrigidos (retificados) do mesmo mês, mantendo dados antigos desatualizados.
3.  **Inconsistência com a Camada de Cache:**
    *   Não há sincronia entre o ETL e o Redis. A carga finaliza sem limpar o cache, fazendo com que a API continue servindo preços e custos obsoletos por até 24 horas.

---

## 2. Plano de Correções (RalphLoops Refactor)

*   **Ação 1:** Implementar a carga nativa via `COPY` do Postgres (especificado em `adrs/001-copy-bulk.md`).
*   **Ação 2:** Adicionar validação de cache Redis com comando `FLUSHDB` integrado na fase final do pipeline (`adrs/002-cache-invalidation.md`).
*   **Ação 3:** Alterar a lógica do `Database._upsert_data` para usar cláusula `ON CONFLICT (codigo, ...) DO UPDATE` com atualização do `sinapi_versao` e timestamp `updated_at`.

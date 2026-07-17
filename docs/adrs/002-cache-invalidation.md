# ADR 002 — Invalidação Proativa de Cache pós-carga do ETL

## Status
Aprovado

## Contexto
Para acelerar o tempo de resposta, a API autoSINAPI implementa uma camada de cache no Redis. Os resultados de buscas textuais de insumos e composições e as consultas de BI são armazenados sob o padrão de chave `cache:*`.

Como o pipeline de ETL roda de forma independente da API (agendamento cron ou acionamento manual), quando novos dados mensais do SINAPI são inseridos no banco, o cache do Redis permanece obsoleto (stale). O consumidor da API continuará recebendo dados e preços do mês anterior até que as chaves expirem por TTL (1 hora ou 24 horas dependendo do endpoint).

## Decisão
Integrar a invalidação do cache no próprio pipeline de ETL (`etl_pipeline.py`).
Imediatamente após a conclusão com sucesso da **Fase 3** (carga no banco de dados e auditoria), o script do ETL deverá:
1.  Conectar-se ao Redis usando as credenciais do ambiente (`REDIS_HOST`, `REDIS_PORT`).
2.  Executar um comando de expurgo para apagar chaves que começam com o prefixo `cache:` (ou, de forma simplificada, rodar um `FLUSHDB` caso a instância do Redis seja dedicada apenas ao cache da API).

## Consequências
*   **Positivas:**
    *   **Coerência de Dados:** Garante que qualquer requisição na API imediatamente após a carga traga as informações e retificações mais recentes do SINAPI.
    *   **Desacoplamento:** A API não precisa de lógica complexa para monitorar mudanças no banco de dados.
*   **Negativas:**
    *   Insere uma nova dependência física (Redis-py) e variáveis de configuração de Redis no escopo da biblioteca de ETL.

# ADR 001 — Ingestão de Alta Performance com Postgres COPY

## Status
Aprovado

## Contexto
O processamento mensal de dados do SINAPI insere centenas de milhares de linhas no PostgreSQL (apenas a tabela de preços de insumos contém ~300.000 registros por estado processado). 
Atualmente, o método `Database._upsert_data` utiliza `pd.to_sql` para carregar dados em uma tabela temporária no banco de dados e, a partir dela, executar uma query de `ON CONFLICT DO UPDATE`. 

Embora funcione, o carregamento via `pd.to_sql` (que converte o DataFrame em múltiplas instruções `INSERT` tradicionais) gera overhead massivo de rede, CPU e disco no banco de dados, resultando em execuções lentas (levando minutos por UF) e consumo excessivo de energia e ciclos de CPU.

## Decisão
Substituiremos a inserção em tabelas temporárias baseada em `pd.to_sql` pelo mecanismo nativo **Postgres `COPY`**.
O pipeline irá:
1.  Gerar um stream em memória (ex: `io.StringIO`) do DataFrame formatado em CSV.
2.  Utilizar o driver do banco para disparar o comando `COPY FROM` para carregar a tabela temporária diretamente no banco.
3.  Executar a query de `ON CONFLICT DO UPDATE` a partir desta tabela temporária.

## Consequências
*   **Positivas:**
    *   **Performance:** Redução do tempo de inserção de dados em até 10x.
    *   **Sustentabilidade Energética:** Redução drástica da utilização de CPU e I/O de disco no servidor de banco de dados.
    *   **Menor concorrência de travas (lock contention):** Conexões de banco de dados são liberadas muito mais rapidamente.
*   **Negativas:**
    *   Requer o uso de drivers de baixo nível (como `pg_copy_to` ou métodos específicos do cursor `psycopg2`/`asyncpg`) em vez de abstrações universais do SQLAlchemy/Pandas.

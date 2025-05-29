import pandas as pd
import os

planilhas = ['familias_e_coeficientes', 'Manutenções', 'mao_de_obra', 'Referência']
formatos = ['xlsx', 'pdf']
ano = '2025'
mes = '03'
# 1. Carregar o arquivo Excel
file_path = '{ano}_{mes}/SINAPI-{ano}{mes}-formato-{formato}/SINAPI_{planilha}_{ano}_{mes}.xlsx'
df = pd.read_excel(file_path)

# 2. Identificar colunas fixas e colunas de estados
fixed_columns = ['mes/ref', 'cod familia', 'cod insumo', 'descr', 'unid', 'categoria']
estados_columns = [col for col in df.columns if col not in fixed_columns]

# 3. Realizar o 'unpivot' (melt) para normalizar os dados
df_normalized = pd.melt(
    df,
    id_vars=fixed_columns,
    value_vars=estados_columns,
    var_name='estado',
    value_name='coeficiente'
)

# 4. Renomear colunas para padrão SQL
df_normalized = df_normalized.rename(columns={
    'mes/ref': 'mes_ref',
    'cod familia': 'cod_familia',
    'cod insumo': 'cod_insumo',
    'descr': 'descricao',
    'unid': 'unidade'
})

# 5. Tratamento de dados (opcional, mas recomendado)
# - Converter tipo de data (se necessário)
# df_normalized['mes_ref'] = pd.to_datetime(df_normalized['mes_ref'], format='%m/%Y')

# - Remover linhas com coeficientes nulos
df_normalized = df_normalized.dropna(subset=['coeficiente'])

# - Converter estado para maiúsculas
df_normalized['estado'] = df_normalized['estado'].str.upper()

# 6. Salvar para CSV (para posterior importação via COPY)
output_csv = 'dados_normalizados.csv'
df_normalized.to_csv(output_csv, index=False, encoding='utf-8')

print(f"Normalização concluída! Dados salvos em: {output_csv}")
print(f"Total de linhas originais: {len(df)}")
print(f"Total de linhas normalizadas: {len(df_normalized)}")
print(f"Exemplo de dados normalizados:")
print(df_normalized.head())

# ---------------------------------------------------------
# 7. (OPCIONAL) Inserção direta no PostgreSQL
# Requer instalação: pip install sqlalchemy psycopg2

from sqlalchemy import create_engine

# Configurações do banco
DB_USER = 'seu_usuario'
DB_PASSWORD = 'sua_senha'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'seu_banco'
TABLE_NAME = 'sinapi_insumos'

# Criar conexão
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Inserir dados diretamente
# df_normalized.to_sql(
#     name=TABLE_NAME,
#     con=engine,
#     if_exists='append',  # ou 'replace' para recriar a tabela
#     index=False,
#     method='multi'  # inserção em lote
# )

# print(f"\nDados inseridos diretamente na tabela {TABLE_NAME}!")
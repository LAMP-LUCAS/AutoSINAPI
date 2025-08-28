# Padrões de Nomenclatura do Projeto AutoSINAPI

Este documento define as convenções de nomenclatura a serem seguidas no desenvolvimento do projeto **AutoSINAPI**, garantindo consistência, legibilidade e manutenibilidade do código.

---

## 1. Versionamento Semântico (SemVer)

O versionamento do projeto segue o padrão Semantic Versioning 2.0.0. O formato da versão é `MAJOR.MINOR.PATCH`.

- **MAJOR**: Incrementado para mudanças incompatíveis com versões anteriores (breaking changes).
- **MINOR**: Incrementado para adição de novas funcionalidades de forma retrocompatível.
- **PATCH**: Incrementado para correções de bugs de forma retrocompatível.

### Versões de Pré-lançamento (Alpha/Beta)

Para versões que não estão prontas para produção, como fases de teste alfa e beta, utilizamos identificadores de pré-lançamento.

- **Alpha**: Versão em desenvolvimento inicial, potencialmente instável e para testes internos. Formato: `MAJOR.MINOR.PATCH-alpha.N` (ex: `1.2.0-alpha.1` ou `0.0.1-alpha.1`).
- **Beta**: Versão com funcionalidades completas, em fase de testes para um público restrito. Formato: `MAJOR.MINOR.PATCH-beta.N` (ex: `1.2.0-beta.1` ou `0.0.1-beta.1`).

O `N` é um número sequencial que se inicia em `1` para cada nova build de pré-lançamento.

**Exemplos:**

- `0.0.1-alpha.1`: Pré-lançamento inicial.
- `0.0.1-beta.1`: Pré-lançamento de testes.
- `1.0.0`: Lançamento inicial.
- `1.1.0`: Adição de suporte para um novo formato de planilha SINAPI (funcionalidade nova).
- `1.1.1`: Correção de um bug no processamento de dados de insumos (correção de bug).
- `2.0.0`: Mudança na estrutura do banco de dados que exige migração manual (breaking change).

---

## 2. Nomenclatura de Branches (Git)

Adotamos um fluxo de trabalho baseado no Git Flow simplificado para organizar o desenvolvimento.

- **`main`**: Contém o código estável e de produção. Apenas merges de `release` ou `hotfix` são permitidos.
- **`develop`**: Branch principal de desenvolvimento. Contém as últimas funcionalidades e correções que serão incluídas na próxima versão.
- **`feature/<nome-da-feature>`**: Para o desenvolvimento de novas funcionalidades.
  - Criada a partir de `develop`.
  - Exemplo: `feature/processar-planilha-insumos` ou `postgres_data-define` para features mais complexas.
- **`fix/<nome-da-correcao>`**: Para correções de bugs não críticos.
  - Criada a partir de `develop`.
  - Exemplo: `fix/ajuste-parser-valor-monetario`
- **`hotfix/<descricao-curta>`**: Para correções críticas em produção.
  - Criada a partir de `main`.
  - Após a conclusão, deve ser mesclada em `main` e `develop`.
  - Exemplo: `hotfix/permissao-acesso-negada`
- **`release/<versao>`**: Para preparar uma nova versão de produção (testes finais, atualização de documentação).
  - Criada a partir de `develop`.
  - Exemplo: `release/v1.2.0`

---

## 3. Mensagens de Commit

Utilizamos o padrão Conventional Commits para padronizar as mensagens de commit.

**Formato:** `<tipo>(<escopo>): <descrição>`

- **`<tipo>`**:
  - `feat`: Uma nova funcionalidade.
  - `fix`: Uma correção de bug.
  - `docs`: Alterações na documentação.
  - `style`: Alterações de formatação de código (espaços, ponto e vírgula, etc.).
  - `refactor`: Refatoração de código que não altera a funcionalidade externa.
  - `test`: Adição ou correção de testes.
  - `chore`: Manutenção de build, ferramentas auxiliares, etc.

- **`<escopo>` (opcional)**: Onde a mudança ocorreu (ex: `import`, `settings`, `charts`).

**Exemplos:**

- `feat(parser): adiciona processamento de planilhas de composições`
- `fix(database): corrige tipo de dado da coluna de preço unitário`
- `docs(readme): atualiza instruções de instalação`
- `refactor(services): otimiza consulta de insumos no banco de dados`

---

## 4. Nomenclatura no Código

### 4.1. CSS (Para clientes Frontend)

- **Prefixo**: Todas as classes devem ser prefixadas com `as-` (AutoSINAPI) para evitar conflitos de estilo.
- **Metodologia**: BEM (Block, Element, Modifier).
  - `as-bloco`
  - `as-bloco__elemento`
  - `as-bloco__elemento--modificador`

**Exemplos:**

- `.as-data-table` (Bloco)
- `.as-data-table__header` (Elemento)
- `.as-data-table__row--highlighted` (Modificador)

### 4.2. Chaves de Internacionalização (I18n - se aplicável)

As chaves de tradução devem seguir uma estrutura hierárquica para facilitar a organização.

- **Padrão**: `auto_sinapi.<area>.<subarea_ou_chave>`

**Exemplos:**

- `auto_sinapi.settings.title`
- `auto_sinapi.dashboard.tables.insumos.title`
- `auto_sinapi.errors.file_format_invalid`

### 4.3. Python (FastAPI)

- **Módulos e Classes**: `PascalCase` (ex: `SinapiParser`, `DatabaseManager`).
- **Variáveis e Funções**: `snake_case` (ex: `file_data`, `process_spreadsheet`).
- **Constantes**: `UPPER_SNAKE_CASE` (ex: `API_VERSION`, `DB_CONNECTION_STRING`).
- **Arquivos**: `snake_case` (ex: `sinapi_parser.py`, `main.py`).
- **Pacotes**: O código deve ser organizado em pacotes e módulos lógicos (ex: `app.services`, `app.models`, `app.routers`).

### 4.4. JavaScript (Para clientes Frontend)

- **Variáveis e Funções**: `camelCase` (ex: `totalAmount`, `initializeFilters`).
- **Classes**: `PascalCase` (ex: `ChartManager`).
- **Constantes**: `UPPER_SNAKE_CASE` (ex: `API_ENDPOINT`).
- **Nomes de Arquivos**: `kebab-case` ou `snake_case` (ex: `data-table.js` ou `data_table.js`).

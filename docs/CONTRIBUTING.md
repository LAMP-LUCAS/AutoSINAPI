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

## 4. Fluxo de Desenvolvimento

Para garantir um desenvolvimento organizado, eficiente e com alta qualidade, seguimos um fluxo de trabalho bem definido, que integra as convenções de nomenclatura de branches e commits já estabelecidas.

### 4.1. Ciclo de Vida de uma Funcionalidade/Correção

1.  **Criação da Branch:**
    *   Para novas funcionalidades: Crie uma branch `feature/<nome-da-feature>` a partir de `develop`.
    *   Para correções de bugs não críticos: Crie uma branch `fix/<nome-da-correcao>` a partir de `develop`.
    *   Para correções críticas em produção: Crie uma branch `hotfix/<descricao-curta>` a partir de `main`.

2.  **Desenvolvimento e Commits:**
    *   Desenvolva a funcionalidade ou correção na sua branch dedicada.
    *   Realize commits frequentes e atômicos, seguindo o padrão de [Mensagens de Commit](#3-mensagens-de-commit). Cada commit deve representar uma mudança lógica única e completa.

3.  **Testes Locais:**
    *   Antes de abrir um Pull Request, certifique-se de que todos os testes locais (unitários e de integração) estão passando.
    *   Execute os linters e formatadores de código para garantir a conformidade com os padrões do projeto.

4.  **Pull Request (PR):**
    *   Quando a funcionalidade ou correção estiver completa e testada localmente, abra um Pull Request da sua branch (`feature`, `fix`, `hotfix`) para a branch `develop` (ou `main` para `hotfix`).
    *   Utilize o template de Pull Request (`.github/pull_request_template.md`) para fornecer todas as informações necessárias, facilitando a revisão do código.
    *   Descreva claramente as mudanças, o problema que resolve (se for um bug) e como testar.

5.  **Revisão de Código e Merge:**
    *   Aguarde a revisão do código por outro(s) membro(s) da equipe.
    *   Enderece quaisquer comentários ou solicitações de alteração.
    *   Após a aprovação, a PR será mesclada na branch de destino (`develop` ou `main`).

### 4.2. Gerenciamento de Releases

O processo de release é automatizado para garantir consistência e agilidade.

1.  **Preparação da Release (Branch `release`):**
    *   Quando um conjunto de funcionalidades e correções na branch `develop` estiver pronto para ser lançado, crie uma branch `release/<versao>` a partir de `develop`.
    *   Nesta branch, realize apenas as últimas verificações, atualizações de documentação (ex: `CHANGELOG.md` se houver) e ajustes finais.

2.  **Criação da Tag de Versão:**
    *   Após a branch `release` estar pronta, crie uma tag de versão seguindo o [Versionamento Semântico](#1-versionamento-semântico-semver) (ex: `v1.0.0`, `v1.1.0`).
    *   **Importante:** O push desta tag para o repositório irá automaticamente disparar o fluxo de trabalho de release.

3.  **Release Automatizada:**
    *   O fluxo de trabalho `.github/workflows/release.yml` será executado automaticamente.
    *   Ele construirá o pacote Python, criará um novo release no GitHub (associado à tag) e publicará o pacote no PyPI.

4.  **Merge Pós-Release:**
    *   Após a release ser concluída com sucesso, a branch `release` deve ser mesclada de volta em `main` (para registrar a versão final) e em `develop` (para garantir que quaisquer ajustes feitos na branch `release` sejam propagados para o desenvolvimento contínuo).

---

## 5. Ferramentas de Automação e Templates

Para otimizar o fluxo de trabalho e garantir a padronização, utilizamos as seguintes ferramentas e templates:

### 5.1. `.github/workflows/release.yml`

Este arquivo define o fluxo de trabalho de **Release Automatizada** do projeto. Ele é um script de GitHub Actions que é executado sempre que uma nova tag de versão (ex: `v1.0.0`) é enviada para o repositório.

**O que ele faz:**
*   **Construção do Pacote:** Compila o código-fonte Python em pacotes distribuíveis (source distribution e wheel).
*   **Criação de Release no GitHub:** Gera um novo lançamento na página de Releases do GitHub, associado à tag de versão.
*   **Publicação no PyPI:** Faz o upload dos pacotes construídos para o Python Package Index (PyPI), tornando-os disponíveis para instalação via `pip`.

**Benefícios:** Garante que cada nova versão seja lançada de forma consistente, reduzindo erros manuais e acelerando o processo de distribuição.

### 5.2. `.github/pull_request_template.md`

Este arquivo é um **template padrão para Pull Requests (PRs)**. Quando um desenvolvedor cria uma nova Pull Request no GitHub, este template é automaticamente preenchido, guiando o desenvolvedor a fornecer as informações essenciais.

**O que ele faz:**
*   **Padronização:** Garante que todas as PRs sigam uma estrutura consistente.
*   **Clareza:** Solicita informações cruciais como descrição das mudanças, tipo de alteração, testes realizados, breaking changes, etc.
*   **Facilita a Revisão:** Ajuda os revisores a entender rapidamente o propósito e o escopo da PR, agilizando o processo de code review.
*   **Checklist:** Inclui um checklist para que o desenvolvedor possa verificar se todos os requisitos foram atendidos antes de submeter a PR.

**Benefícios:** Melhora a qualidade das PRs, acelera o processo de revisão e contribui para a manutenção de um histórico de projeto claro e detalhado.

---

## 6. Nomenclatura no Código


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

# 🚀 AutoSINAPI: Transformando Dados em Decisões Estratégicas na Construção Civil

[![Licença](https://img.shields.io/badge/licen%C3%A7a-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/LAMP-LUCAS/AutoSINAPI/releases)

O **AutoSINAPI** é uma solução open-source completa para profissionais de Arquitetura, Engenharia e Construção (AEC) que buscam eficiência e precisão na gestão de custos. Ele automatiza todo o ciclo de vida dos dados do SINAPI, desde a coleta até a análise, transformando um processo manual e demorado em um pipeline de dados robusto e confiável.

Com o AutoSINAPI, você para de gastar horas com planilhas e foca no que realmente importa: **análises estratégicas, orçamentos precisos e decisões baseadas em dados.**

---

## 1. O Que o AutoSINAPI Faz por Você?

O AutoSINAPI foi criado para resolver um dos maiores gargalos dos profissionais de AEC: o acesso e a manipulação dos dados do SINAPI. Nossa solução oferece um ecossistema completo para automação de ponta a ponta.

### O Que Ele Pode Fazer

-   **Automação Completa do Pipeline de Dados:** Baixe, processe e organize os dados do SINAPI de forma automática, eliminando tarefas manuais repetitivas e reduzindo a chance de erros.
-   **Estruturação Inteligente de Dados:** Converta as complexas planilhas do SINAPI em um banco de dados PostgreSQL estruturado, pronto para ser consumido por qualquer ferramenta de análise, BI ou sistema interno.
-   **Foco em Produtividade e Eficiência:** Ganhe tempo e aumente a precisão dos seus orçamentos com acesso rápido a dados atualizados e consistentes.
-   **Análises Históricas Simplificadas:** Com os dados organizados em um banco de dados, você pode facilmente analisar tendências de custos, comparar períodos e tomar decisões mais informadas.

### Como Ele Faz

O AutoSINAPI opera através de um pipeline de ETL (Extração, Transformação e Carga) inteligente e automatizado:

1.  **Extração (Download Inteligente):** O robô do AutoSINAPI primeiro verifica se o arquivo do mês de referência já existe localmente. Se não existir, ele baixa as planilhas mais recentes diretamente do site da Caixa Econômica Federal.
2.  **Transformação (Processamento):** As planilhas são lidas, limpas e normalizadas. Os dados são validados e estruturados de acordo com um modelo de dados relacional, otimizado para consultas e análises.
3.  **Carga (Armazenamento Seguro):** Os dados transformados são carregados no banco de dados PostgreSQL. O pipeline verifica a política de duplicatas no seu arquivo de configuração para evitar a inserção de dados duplicados, garantindo a integridade da sua base de dados.

O resultado é um banco de dados sempre atualizado, pronto para ser a fonte de verdade para seus orçamentos e análises.

---

## 2. Instalação e Atualização

### Instalação Inicial

Para começar a usar o AutoSINAPI, siga os passos abaixo.

**Pré-requisitos**

-   Python 3.8 ou superior
-   PostgreSQL 12 ou superior

**Passo a Passo**

1.  **Clone o repositório:**

    ```bash
    git clone https://github.com/LAMP-LUCAS/AutoSINAPI.git
    cd AutoSINAPI
    ```

2.  **Crie e ative um ambiente virtual:**

    ```bash
    # Crie o ambiente
    python -m venv venv

    # Ative no Windows
    .\venv\Scripts\activate

    # Ative no Linux ou macOS
    source venv/bin/activate
    ```

3.  **Instale o AutoSINAPI e suas dependências:**

    ```bash
    pip install .
    ```

### Atualizando o Módulo

Para atualizar o AutoSINAPI para a versão mais recente, navegue até a pasta do projeto e use o `git` para obter as últimas alterações e, em seguida, reinstale o pacote:

```bash
git pull origin main
pip install .
```

---

## 3. Aplicação do Módulo: Configuração e Uso

Com o AutoSINAPI instalado, o próximo passo é configurar e executar o pipeline de ETL.

### 1. Configure o Acesso ao Banco de Dados

-   Na pasta `tools`, renomeie o arquivo `sql_access.secrets.example` para `sql_access.secrets`.
-   Abra o arquivo `sql_access.secrets` e preencha com as credenciais do seu banco de dados PostgreSQL.

### 2. Crie seu Arquivo de Configuração

- Copie o arquivo `tools/CONFIG.example.json` para um novo arquivo (por exemplo, `meu_config.json`).
- Edite o seu novo arquivo de configuração com os parâmetros desejados.

### 3. Execute o Pipeline de ETL

Use o script `autosinapi_pipeline.py` para iniciar o processo, especificando o seu arquivo de configuração com a flag `--config`.

**Exemplo de uso:**

```bash
python tools/autosinapi_pipeline.py --config tools/meu_config.json
```

---

## 4. Versionamento e Estratégia de Lançamento

O versionamento deste projeto é **totalmente automatizado com base nas tags do Git**, seguindo as melhores práticas de integração e entrega contínua (CI/CD).

-   **Versões Estáveis:** Qualquer commit marcado com uma tag (ex: `v0.1.0`) será automaticamente identificado como uma versão estável com aquele número.
-   **Versões de Desenvolvimento:** Commits entre tags são considerados versões de desenvolvimento e recebem um número de versão dinâmico (ex: `0.1.1.dev1+g<hash>`).

Isso garante que a versão instalada via `pip` sempre corresponda de forma transparente ao código-fonte no repositório.

## 🌐 Ecossistema AutoSINAPI

O AutoSINAPI não para no ETL. Para facilitar ainda mais o consumo dos dados, criamos uma API RESTful pronta para uso:

-   **[autoSINAPI_API](https://github.com/LAMP-LUCAS/autoSINAPI_API):** Uma API FastAPI para consultar os dados do banco de dados SINAPI de forma simples e rápida.

## 🤝 Como Contribuir

Este é um projeto de código aberto. Contribuições são bem-vindas! Dê uma olhada no nosso [repositório no GitHub](https://github.com/LAMP-LUCAS/AutoSINAPI) e participe.

## 📝 Licença

O AutoSINAPI é distribuído sob a licença **GNU General Public License v3.0**.

# 🚀 AutoSINAPI: Transformando Dados em Decisões Estratégicas na Construção Civil

[![Licença](https://img.shields.io/badge/licen%C3%A7a-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/LAMP-LUCAS/AutoSINAPI/releases)

O **AutoSINAPI** é uma solução open-source completa para profissionais de Arquitetura, Engenharia e Construção (AEC) que buscam eficiência e precisão na gestão de custos. Ele automatiza todo o ciclo de vida dos dados do SINAPI, desde a coleta até a análise, transformando um processo manual e demorado em um pipeline de dados robusto e confiável.

Com o AutoSINAPI, você para de gastar horas com planilhas e foca no que realmente importa: **análises estratégicas, orçamentos precisos e decisões baseadas em dados.**

---

## Como Usar o AutoSINAPI

Existem duas maneiras de rodar o pipeline, escolha a que melhor se adapta ao seu fluxo de trabalho.

### Opção 1: Ambiente Docker (Recomendado)

A forma mais simples e recomendada de usar o AutoSINAPI. Com um único comando, você sobe um ambiente completo e isolado com o banco de dados PostgreSQL e o pipeline pronto para rodar.

**Pré-requisitos:**
-   Docker e Docker Compose instalados.

**Passo a Passo:**

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/LAMP-LUCAS/AutoSINAPI.git
    cd AutoSINAPI
    ```

2.  **Configure o Ambiente:**
    -   Dentro da pasta `tools/docker/`, renomeie o arquivo `.env.example` para `.env`.
    -   Abra o arquivo `.env` e ajuste as variáveis conforme sua necessidade (ano, mês, senhas, etc.).

3.  **(Opcional) Adicione Arquivos Locais:**
    -   Se você já tiver o arquivo `.zip` do SINAPI, coloque-o dentro da pasta `tools/docker/downloads/`. O pipeline irá detectá-lo, renomeá-lo para o padrão correto (se necessário) e pulará a etapa de download.

4.  **Execute o Pipeline:**
    Ainda dentro da pasta `tools/docker/`, execute o comando:
    ```bash
    docker-compose up
    ```
    Este comando irá construir a imagem, subir o container do banco de dados e, em seguida, rodar o container da aplicação que executará o pipeline. Ao final, os containers serão finalizados.

### Opção 2: Ambiente Local (Avançado)

Para quem prefere ter controle total sobre o ambiente e não usar Docker.

**Pré-requisitos:**
-   Python 3.8+ e PostgreSQL 12+ instalados e configurados na sua máquina.

**Passo a Passo:**

1.  **Clone o repositório e instale as dependências** conforme a seção de instalação do `README.md`.
2.  **Configure o acesso ao banco de dados** no arquivo `tools/sql_access.secrets`.
3.  **Crie e ajuste um arquivo de configuração** (ex: `tools/meu_config.json`) a partir do `tools/CONFIG.example.json`.
4.  **Execute o pipeline** via linha de comando:
    ```bash
    python tools/autosinapi_pipeline.py --config tools/meu_config.json
    ```

---

## Versionamento e Estratégia de Lançamento

O versionamento deste projeto é **totalmente automatizado com base nas tags do Git**. Para mais detalhes, consulte a documentação sobre o fluxo de trabalho do Git.

## 🌐 Ecossistema AutoSINAPI

-   **[autoSINAPI_API](https://github.com/LAMP-LUCAS/autoSINAPI_API):** API para consumir os dados do banco de dados SINAPI.

## 🤝 Como Contribuir

Contribuições são bem-vindas! Consulte o nosso [repositório no GitHub](https://github.com/LAMP-LUCAS/AutoSINAPI).

## 📝 Licença

Distribuído sob a licença **GNU General Public License v3.0**.
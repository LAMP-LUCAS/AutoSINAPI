# Changelog

Todas as mudanças notáveis deste projeto são documentadas neste arquivo.
Formato baseado em [Keep a Changelog](https://keepachangelog.com/) e versionamento
[Semantic Versioning](https://semver.org/).

## [v0.5.0-beta.0] - 2026-07-17

Release da branch `develop` inteira, acumulando as mudanças desde `v0.4.0-beta.0`.
Inclui evolução do ETL (enriquecimento, rastreabilidade, sandbox), hardening de SSOT
e reestruturação da documentação.

### Added
- **ETL Smart Discovery**: descoberta automática de arquivos XLSX locais
  (`PipelineETL._discover_local_files`) — `autosinapi/etl_pipeline.py`
- **Enriquecimento (classificação/grupo)** e **SSOT hardening** (famílias, coeficientes,
  labor-mix) — `autosinapi/etl_pipeline.py`
- **Rastreabilidade ETL** completa (run_id UUID, tabelas de estrutura) — `autosinapi/core/database.py`
- **Sandbox ETL** populado e verificado com 1,16M registros
- **Trends multi-dimensão** na API/UI com endurecimento da integridade de dados
- População de `data_referencia` derivada de `sinapi_versao` quando ausente — `autosinapi/core/database.py`
- Suíte de testes de rastreabilidade e isolamento sandbox — `tests/`
- Documentação reestruturada: ADRs (`docs/adrs/`), `docs/architecture.md`, `docs/prd.md`,
  `docs/workPlan.md`, `docs/loops/AUDIT_REPORT.md`, `docs/README.md`

### Fixed
- Caminho do CSV de custos corrigido (`csv_dir`) — `autosinapi/core/processor.py`
- Linha de cabeçalho das planilhas de famílias/MO de `header=4` → `header=5`
  — `autosinapi/core/processor.py`
- **Deduplicação antes do upsert** para evitar `CardinalityViolation` no `ON CONFLICT`
  — `autosinapi/core/database.py`
- Compatibilidade com Python 3.8 (`typing.List` no pre-processor)
- Uso de nomes de tabelas do config no modo sandbox
- Restauração da dependência `xlsxwriter` e upload de codecov não-bloqueante (CI)

### Changed
- Workflow de release: publicação no PyPI desativada (bypass) — `.github/workflows/release.yml`

### Removed
- `tests/test_file_input.py` (substituído pela nova suíte de pipeline)

## [v0.4.0-beta.0] - 2026-05-25

Rastreabilidade Total & SSoT Hardening. Veja histórico anterior em `git log`.

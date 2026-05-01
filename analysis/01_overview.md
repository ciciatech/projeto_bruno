# FASE 1: INVENTÁRIO — O que é este projeto?

## Identificação Básica

| Aspecto | Valor |
|---------|-------|
| **Nome** | `projeto_bruno` (academico-bruno) |
| **Tipo** | Pipeline de Dados + Dashboard de Visualização |
| **Propósito** | Tese de Doutorado: Impactos do Crédito no Crescimento Econômico do Nordeste |
| **Autor** | Bruno Cardoso Costa |
| **Orientador** | Prof. Dr. Magno Prudencio de Almeida Filho |
| **Programa** | DESP/UFC (Doutorado Profissional em Economia do Setor Público) |
| **Status** | Em desenvolvimento ativo (defesa prevista março/2028) |
| **Período de análise** | 2015-2025 (11 anos) |
| **Repositório** | https://github.com/ciciatech/projeto_bruno.git |
| **Produção** | Coolify VPS (72.60.152.227 — Hostinger), com Healthcheck integrado |

---

## Stack Tecnológico Identificado

### Backend
- **Linguagem:** Python 3.13.3
- **Runtime:** 9.136 linhas de código distribuído em 19 módulos
- **Estrutura:** Modular (extract → transform → quality)
- **Dependências principais:**
  - `streamlit==1.54.0` (visualização frontend)
  - `pandas==2.3.3` (processamento de dados)
  - `plotly==6.5.2` (gráficos interativos)
  - `pyarrow==23.0.1` (serialização Parquet)
  - `requests==2.32.5` (requisições HTTP)
  - `py7zr>=0.20.0` (descompressão)

### Frontend
- **Tipo:** Streamlit (app web em Python puro)
- **Arquitetura:** 1 página raiz (`app.py`) + 10 sub-páginas (padrão Streamlit `pages/`)
- **Configuração:** Headless, sem stats, tema light customizado

### Infraestrutura
- **Container:** Docker (imagem `python:3.13-slim`)
- **Orquestração:** Docker Compose (1 serviço: `streamlit`)
- **Porta:** 8501 (Streamlit padrão)
- **Healthcheck:** Curl para `/_stcore/health` a cada 30s
- **Deploy:** Coolify (Pull automático do GitHub → Build → Run)

---

## Mapa de Fontes de Dados (9 Datasets)

| # | Fonte | API/Protocolo | Cobertura Espacial | Periodicidade | Linhas Brutas | Status |
|---|-------|---------------|-------------------|----------------|----|--------|
| 1 | **BACEN-SGS** | REST API | Brasil (nacional) | Mensal | 132 | ✅ Completo |
| 2 | **SICONFI RREO** | REST API | 9 UFs NE | Bimestral | 1.987.525 | ✅ Completo |
| 3 | **SICONFI RGF** | REST API | 9 UFs NE | Quadrimestral | 95.760 | ✅ Completo |
| 4 | **SICONFI DCA** | REST API | 9 UFs NE | Anual | 232.661 | ✅ Completo |
| 5 | **Transferências** | Derivado (RREO) | 9 UFs NE | Bimestral | 42.205 | ✅ Completo |
| 6 | **CAGED Antigo** | FTP (7z) | 9 UFs NE | Mensal | 423 meses | ⚠️ 13 gaps |
| 7 | **CAGED Novo** | FTP (7z) | 9 UFs NE | Mensal | 648 meses | ✅ Completo |
| 8 | **RAIS** | FTP (7z) | 9 UFs NE | Anual | 47 anos + setorial | ⚠️ 2019-20 gaps |
| 9 | **SIOF-CE** | WebForm ASP.NET | Ceará | Anual | Variável | ✅ Com obras |

---

## Estrutura de Arquivos Crítica

```
projeto_bruno/
├── app.py                          # Página raiz (7-page index)
├── pages/                          # 10 páginas Streamlit
│   ├── 1_BACEN_Indicadores.py     # Dashboard BACEN
│   ├── 2_Bolsa_Familia.py         # Transferências federais
│   ├── 3_SICONFI_RREO.py          # Receitas/despesas
│   ├── ...                        # (7 páginas mais)
│   └── 10_CAGED_RAIS.py           # Emprego formal
│
├── pipeline/                       # 9.136 linhas de código
│   ├── config.py                  # Constantes, paths, UFs
│   ├── utils.py                   # Logging, salvamento
│   ├── run.py                     # Orquestrador principal (246 linhas)
│   ├── quality.py                 # Auditoria automatizada (616 linhas)
│   │
│   ├── extract/                   # Coletores de dados (1.700+ linhas)
│   │   ├── bacen.py              # BACEN-SGS
│   │   ├── siconfi.py            # SICONFI (RREO, RGF, DCA)
│   │   ├── caged_rais.py         # CAGED + RAIS via FTP (965 linhas)
│   │   ├── transferencias.py     # Transferências
│   │   ├── bolsa_familia.py      # Bolsa Familia (fallback SAGI/Portal)
│   │   ├── portal_transparencia.py  # Portal Transparência Brasil
│   │   ├── transparencia_al.py   # Alagoas
│   │   ├── transparencia_pi.py   # Piauí
│   │   ├── siof.py               # SIOF-CE (WebForm scraping)
│   │   └── __init__.py
│   │
│   ├── transform/                # ETL e preparação (1.300+ linhas)
│   │   ├── etl.py               # Raw → Processed (623 linhas)
│   │   ├── preparacao_modelo.py # Deflacionamento + harmonização (593 linhas)
│   │   └── __init__.py
│   └── __init__.py
│
├── dados_nordeste/
│   ├── raw/                       # ~101 CSVs brutos (ignorados por .gitignore)
│   ├── processed/                 # ~134 CSVs processados (4.2 MB commitados)
│   │   ├── bacen/                 # 60K
│   │   ├── caged/                 # 396K (principal: emprego)
│   │   ├── rais/                  # 140K (estoque emprego)
│   │   ├── siconfi_rreo/          # 364K (receitas)
│   │   ├── siconfi_rgf/           # 2.2M (dívida consolidada)
│   │   ├── siconfi_dca/           # 112K (balanço)
│   │   ├── transferencias/        # 1.2M
│   │   ├── execucao_orcamentaria/ # 52K (CE, AL, PI)
│   │   ├── coleta_status/         # Metadados
│   │   └── model_ready/           # 320K (9 arquivos finais para modelagem)
│   ├── quality/                   # Relatórios de qualidade (JSON, MD, CSV)
│   ├── logs/                      # Executados em ./logs
│   └── metadata_coleta.json       # Última execução (atualizado 2026-04-15)
│
├── docs/                          # Documentação
│   ├── README.md                  # (This file, acima)
│   ├── dicionario_dados.md        # Dicionário de 50+ variáveis
│   ├── relatorio_qualidade.md     # Qualidade por fonte
│   ├── analise_projeto.md         # Redirecionamento tese + gaps
│   ├── analise_tecnica_rag.md     # Sistema RAG (BNB) — não commitado em master
│   └── OBSIDIAN_CONSOLIDADO.md    # Anotações de pesquisa
│
├── notebook/                      # Jupyter notebooks (exploratório)
├── Dockerfile                      # Python 3.13 + pip install
├── docker-compose.yml             # 1 serviço: streamlit
├── .streamlit/config.toml         # Theme, headless
├── .gitignore                     # Ignora raw/**/*.csv, *.7z, venv
├── requirements.txt               # 8 dependências
│
├── .git/                          # 12 commits, 2 branches (dev, main)
└── .claude/                       # (vazio — sem CLAUDE.md)
```

---

## Idade e Atividade do Projeto

| Métrica | Valor |
|---------|-------|
| **Commits** | 12 commits desde fev/2024 |
| **Período** | fev/24 → abr/26 (2+ anos) |
| **Frequência** | ~1 commit a cada 2 meses (ativo mas lento) |
| **Branches ativas** | `main` (prod) + `dev` (trabalho) |
| **Último commit** | `d3eb09c` (15/04/2026): "fix: migrar dashboard de parquet para CSV" |
| **Contributores** | 1 (Cassio Pinheiro — cassio@ciciatech.dev) |

### Timeline de Marcos
1. **fev/24**: Estrutura Docker + Streamlit inicial
2. **mar/24**: Dashboard BACEN
3. **abr/25**: Pipeline modular (extract → transform)
4. **fev/26**: Redirecionamento: emprego como dependente
5. **mar/26**: Quality layer + model_ready
6. **abr/26**: Migração parquet → CSV, SIOF-CE com obras

---

## Tamanho e Complexidade do Codebase

| Categoria | Linhas | Arquivos | Complexidade |
|-----------|--------|----------|--------------|
| Pipeline (código) | 9.136 | 19 py | Modular |
| Dashboard (código) | 2.100+ | 1 raiz + 10 páginas | Bem organizado |
| Documentação (código) | Inline docstrings | Python | Parcial |
| **Total de código** | ~11.000+ | ~30 arquivos | **Médio** |
| Dados (CSV processados) | 4.2 MB | 134 arquivos | Bem segmentado |

### Estrutura de Complexidade
- **Extrator mais complexo:** `caged_rais.py` (965 linhas) — FTP + descompressão + lógica multi-formato
- **Transformação mais pesada:** `etl.py` (623 linhas) — pivotagem, normalização, agregação
- **Auditoria:** `quality.py` (616 linhas) — 19 verificações automáticas

---

## Fluxo de Execução Principal

```
PipelineColeta.executar()
├── BackenSGS.coletar_todas()        → dados_nordeste/raw/bacen/bacen.csv
├── Siconfi.coletar_rreo_nordeste()  → raw/siconfi_rreo/
├── Siconfi.coletar_rgf_nordeste()   → raw/siconfi_rgf/
├── CagedRais.coletar()              → raw/caged/, raw/rais/
└── [...]
    ├── ETL: raw → processed/
    ├── Model-Ready: deflacionamento + harmonização
    └── Quality: auditoria de 19 datasets
```

**Modes:**
- `--apenas-bacen` → 1 série rápida
- `--modulos X Y Z` → subset específico
- `--full` → tudo (leva ~2-3 horas FTP)
- `--etl` → transformação apenas
- `--auditar` → quality report apenas

---

## Produção (Coolify)

**Status:** Rodando em VPS Hostinger 72.60.152.227

### Deploy
- **Método:** Git push → Coolify webhook → Docker build + run
- **Branch:** origin/main (production branch)
- **Port:** 8501
- **Healthcheck:** Ativo (curl /_stcore/health a cada 30s)
- **Restart:** unless-stopped

### Dados em Produção
- CSV processados commitados no repo (4.2 MB)
- Raw data NÃO commitado (ignorado por .gitignore)
- Lógica: Coolify roda com dados estáticos no processed/

---

## Status Técnico do Projeto

### ✅ Força
1. **Documentação forte:** README, dicionário, relatório de qualidade
2. **Coleta robusta:** 9 fontes APIs cobertas, com fallbacks
3. **Estrutura modular:** Extract, Transform, Quality bem separados
4. **Dados model-ready:** Painel final 594x37 (bimestral, 9 UFs)
5. **Deploy funcional:** Coolify + Docker + healthcheck
6. **Idempotente:** Reexecução sobrescreve, sem side effects

### ⚠️ Fragil (Riscos)
1. **RAIS quebrada:** Gaps em 2019-20, sem remuneração (100% nulo)
2. **CAGED Antigo:** 13 meses faltando (7z corrompidos no FTP)
3. **SIOF reduzido:** Apenas 3 de 9 estados (CE, AL, PI)
4. **RN, PB, PE, MA, BA, SE:** Sem execução orçamentária
5. **Dependência FTP frágil:** Taxa de sucesso ~85% (rate limits, timeouts)
6. **BPC não coletado:** Controle assistencial pendente

### 🔴 Crítica
1. Nenhuma autenticação no Streamlit (público na VPS)
2. Sem SLA de coleta (cron job desconhecido)
3. Sem backups dos dados raw (apenas processados commitados)
4. API key Portal Transparência em variável de env (seguro, mas documentação faltante)

---

## Conclusão da Fase 1

**Tipo:** Projeto acadêmico em produção leve (dashboard + pipeline ETL)  
**Maturidade:** Média (documentado, modular, mas frágil em alguns coletores)  
**Risco:** Médio (dados incompletos, FTP instável, sem autenticação)  
**Próximo passo:** Fase 2 — Arquitetura e dependências

---

**Fontes:** git log, Dockerfile, requirements.txt, app.py, pipeline/config.py, docs/README.md, github://ciciatech/projeto_bruno

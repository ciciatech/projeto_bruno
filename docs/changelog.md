# Changelog · Prisma Regional CE

Histórico das principais entregas. Datas em America/Fortaleza (UTC-3).

## 2026-04-30 / 2026-05-01 — Sprint Prisma Regional CE

Sessão maratona movendo o projeto da fase "Streamlit + 27 UFs sintéticas"
para "React/Vite + 14 regiões CE com dados reais". 4 deploys em produção.

### Backend (pipeline ETL)

- `pipeline/extract/invest_municipal_siconfi.py` — coletor SICONFI Anexo 01,
  bimestral acumulado-no-ano → fluxo mensal, com auditoria anti-relato-corrente
  (commit `59ca1f9`). Em execução no Mac Mini quando esta sessão fechou (PID
  82781, ~31% às 23:38 do dia 30).
- `pipeline/extract/sefaz_ce_siconfi.py` — Cota-Parte ICMS/IPVA via RREO Anexo
  03 (12-mês window pelo bimestre 6). Substitui o adapter manual do Ceará
  Transparente (bloqueado por WAF). Validado em Fortaleza 2024 = R$ 1.76 bi
  (commit `0a61aa1`).
- `pipeline/extract/populacao_ibge.py` — IBGE SIDRA 6579 (Estimativas
  Populacionais). 1656 linhas, 184 munic × 9 anos disponíveis. Habilita
  Per capita no frontend (commit `215c6b2`).
- `pipeline/transform/preparacao_modelo_regional.py` — fix do bug
  `agregar_para_regiao` quebrava com KeyError quando o df de entrada já
  tinha `regiao_codigo` (commit `7819802`). E `_carregar_transf_estadual`
  agora prefere SICONFI sobre adapter manual.
- `pipeline/config.py` — `PERIODO_FIM_MENSAL = 2026` para incluir o ano
  corrente do SIOF SEPLAG no painel (commit `1a0d9f6`).

### Frontend (React/Vite)

- **Stack provisionada**: React 19 + Vite + TypeScript + Tailwind v4 +
  react-router-dom + simple-statistics (commit `f9d813d`, `8fc1568`).
- **5 telas implementadas**:
  - **Investimento** — dados reais 14 regiões CE (SIOF + invest_federal +
    IBCR + FBCF), composição com 4 esferas (Estadual/Federal/Municipal/
    Privado residual via FBCF×2.2%), MapLegend, MapTooltip, "Por zona".
  - **Emprego** — CAGED real (9/14 regiões), mapa divergente, KPIs, snapshot.
  - **Setores** — pivot para "Composição de Receitas Públicas Regionais"
    (FPM/FUNDEB/royalties/ITR/outros + BF + BPC), stacked bars 14 regiões.
  - **Causal** — OLS univariado preliminar (siof_emp → sal saldo CAGED) com
    `simple-statistics`, scatter + linha + IC 95% + tabela β/α/R²/σ.
  - **Pipeline** — 11 fontes com badges (OK / RODANDO / PENDENTE / BLOQUEADO).
- **FilterBar** com período (1A/3A/5A/10A/Tudo) + Per capita habilitado,
  estado em URL (`?periodo=5A`).
- **Cache HTTP** via hash content-addressed (`painel.{hash}.json` 1y +
  `painel-index.json` no-cache).
- **Tema dark/light** persistente em localStorage.
- **Layout responsivo**: `minmax(260px, 320px)` nas colunas, Choropleth com
  aspect-ratio nativo, Panel com `overflow:auto` (corrige clipping
  silencioso em viewports <1440px).
- Bug fix de composição (Federal inflava 14× porque é estadual replicado
  nas 14 regiões — commit `ed7e377`).

### Infraestrutura

- App Coolify `prisma-frontend` (`eomewrww9ecurlqvhb6vusml`) provisionada via
  API. fqdn `prisma.bruno.ciciatech.cloud` com DNS Hostinger e TLS
  Let's Encrypt.
- GitHub Action atualizada com job paralelo para deploy das duas apps
  (`bruno-dashboard` Streamlit + `prisma-frontend` React).
- `scripts/quality-gate.sh` adaptado ao projeto (lint frontend + tsc + pytest
  com cobertura ≥10% sobre `pipeline/` + secrets check).
- 24 testes (5 pytest smoke + 19 Vitest).
- `.claude/` completa (rules, agents, commands, task-pilot, settings) +
  `.mcp.json.example` para Hostinger MCP (token fora do repo).

### Documentação

- `CLAUDE.md` — entrypoint do projeto.
- `tasks.md` — roadmap consolidado.
- `docs/plano-descontinuacao-streamlit.md` — 4 etapas para retirar legado.
- `docs/metodologia-composicao-investimento.md` — citação dos 3 áudios do
  Prof. Paulo (a fórmula `inv_total = FBCF × 2.2%` deve ser referenciada
  em qualquer publicação).
- `archive/prisma-regional-design/README.md` — mapa de port (JSX original
  → componente atual).
- `scripts/qa-prisma-prompt.md` + `scripts/qa-prisma-ux.md` — prompts para
  QA visual em browser tool.

### Decisões resolvidas (todas pelos áudios do Prof. Paulo, abr/2026)

- Fonte do "investimento total privado": **FBCF Brasil mensal × 2.2% (share
  PIB CE/BR)**, em R$ presente de dez/2024.
- Investimento federal: **3 componentes do RREO** (direto + NE×14.5% +
  nacional×2.2%) — já estava implementado.
- CAGED municipal: **vale o custo** (rodou em 59min, não 24h).
- Investimento municipal: **SICONFI automático**, não planilha manual.
- SEFAZ-CE: **destravado via SICONFI Anexo 03** (cota-parte ICMS/IPVA).

### Pendências críticas

- **Especificação econométrica completa** (Tela 4) — Prof. Paulo precisa
  definir lags, transformações, IV/Granger, controles.
- **Deflator IPCA** para harmonizar bases monetárias (SIOF correntes,
  FBCF R$ 2010, federal correntes) → R$ dez/2024.
- **Previsão FBCF 2025/2026** — IpeaData não publicou ainda.

### Em curso quando a sessão fechou

- Coletor SICONFI invest_municipal no Mac Mini (PID 82781). ETA ~04:30 BRT
  do dia 1º de maio. Quando concluir, `run_coletas.sh` encadeia
  `--painel-ce` que regenera o painel com **30+ colunas** + SIOF 2026 +
  Municipal SICONFI. Próximo deploy automático.

### Métricas finais da sessão

- **27 commits** entre `59ca1f9` (10:30 BRT 30/04) e `3e75cad` (00:40 BRT
  01/05).
- **6 deploys** Coolify do `prisma-frontend`, todos automáticos via
  GitHub Action.
- **2 bugs latentes** descobertos e corrigidos (`agregar_para_regiao`
  KeyError; composição inflada 14×).
- **3 root causes** de "layout desajustado" diagnosticados via QA estático
  (grid rígido, Choropleth aspect ratio, overflow:hidden).

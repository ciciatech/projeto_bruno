# Changelog · Doutorado Bruno — Investimento Público e Emprego no CE (codinome interno: Prisma Regional)

Histórico das principais entregas. Datas em America/Fortaleza (UTC-3).

## 2026-06-10 (noite) — Correção pós-validação: continuidade de meses por fonte

- **CAGED 2025-03 recuperado**: a recoleta da tarde perdeu o mês 202503 por
  falha transitória do FTP do MTE (`550 semaphore timeout`, tratada como
  WARNING) e publicou o painel com "135 meses" como sucesso — o bimestre 25b2
  saía com fluxos ~metade (admissões estaduais 56 mil vs 112-121 mil dos
  vizinhos). Re-download OK (183 municípios, saldo -2.621); série agora
  CONTÍNUA 2015-01→2026-04 (136 meses, 23.987 linhas). Regenerados painel
  mensal, bimestral, xlsx do Prof. Paulo e JSON do frontend.
- **Guard de continuidade**: `pipeline/utils.meses_faltantes_interior` +
  ERROR nos coletores CAGED municipal e STN quando há mês interior sem dado;
  teste `test_continuidade_mensal_por_fonte` cobre TODAS as fontes da matriz
  (lacunas aceitas só se documentadas).
- **Auditoria estendida ao STN** (mesma classe de falha, pré-existente):
  7 meses interiores faltavam desde sempre. Corrigidos 2 (bug nosso):
  **2018-05** — arquivo publicado sem a coluna "Transferência"; destino
  reconstruído por inferência item+ordem de ocorrência, validada com zero
  divergências nos arquivos bem formados 2018-2021 (`ValueError` para itens
  fora do universo validado); **2021-08** — arquivo corrigido pela fonte
  desde a coleta original. Documentados 5 (limitação da fonte): **2020-11,
  2022-10, 2023-11, 2024-11, 2025-11** nunca tiveram o conteúdo publicado
  (o CKAN/STN publica o arquivo `AAAAMM` com conteúdo `AAAAMM-1` de ~set a
  ~nov e re-sincroniza em dez; evidência: adicional de 1% do FPM de setembro,
  EC 84/2014). Bimestres 20b6, 22b5, 23b6, 24b6 e 25b6 **anulados** nas
  `transf_fed_*` (meio-bimestre não passa por valor válido — mesma política
  do gate de outliers); documentação na matriz de regras e no Leia-me do xlsx.
- Suíte: 73 pytest + 19 vitest verdes.

## 2026-06-10 — Sessão 3: painel bimestral + deflator + revisão adversarial

- Planilha bimestral do Prof. Paulo Matos ingerida e organizada em
  `docs/dados_prof_paulo/` (T19-T21, T26); 14 regiões, R$ dez/25. Geram-se
  **4 confirmações pendentes** com ele: proxy cota-parte ICMS, share federal
  15,4% regional (vs 14,5% NE), base dez/25 vs dez/24, EMPENHADO vs PAGO.
- **T27** `pipeline/transform/painel_bimestral.py` — painel bimestral regional
  (`painel_regional_ce_bimestral.csv`, 1008×56, 15b1–26b6, 20 colunas `_real`)
  + export xlsx no layout do Paulo. 28/28 testes pytest verdes à época (suíte do dia fechou em 38).
- **T28** `pipeline/transform/deflator.py` — IPCA base pinada dez/25
  (parametrizável); reproduz os 66 deflatores da planilha com erro 2×10⁻¹⁵.
- Revisão adversarial → **T32-T37**: outlier transf_fed set/24 (anulado em 24b5),
  CAGED 18/184 municípios (bug 6→7 dígitos corrigido em `d05162c` — o "9/14
  regiões" da entrada de abril era artefato; recoleta em curso), invest. municipal
  empenhado vs pago, recoleta `sefaz_ce_siconfi` completa, grafia "Maciço de Baturité".
- Desde a última entrada: **Etapa B-swap** concluída em 2026-05-02 —
  `bruno.ciciatech.cloud` passou ao `prisma-frontend`; Streamlit sem domínio.

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

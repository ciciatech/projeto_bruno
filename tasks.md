# Tasks · Prisma Regional / Tese DESP-UFC

Roadmap de tarefas. Sincronizado com `.claude/task-pilot/tasks.md`.

> ✅ done · ⏳ em curso · ⏸ aguardando · 🚧 bloqueado por terceiros

> Histórico detalhado da sprint inicial em `docs/changelog.md`.

---

## Concluídas

- ✅ **T02** Tela 2 Emprego com CAGED real — mapa divergente, KPIs, tabela 14 regiões. Cobertura 9/14.
- ✅ **T04** Tela 3 — pivot para "Composição de Receitas Públicas Regionais" (FPM/FUNDEB/royalties/ITR/outros + BF + BPC).
- ✅ **T05** Tela 4 Causal — OLS univariado real com `simple-statistics`, scatter + linha + IC 95% + tabela β/α/R²/σ. **Especificação preliminar** (multivariada/IV/Granger aguarda Paulo).
- ✅ **T06** FilterBar funcional (período + recorte com URL state via react-router).
- ✅ **T08** Decisão sobre invest_privado residual — RESOLVIDA pelos áudios do Paulo (abr/2026): FBCF Brasil mensal × 2,2% (share PIB CE/BR), R$ presente de dez/2024. Documentado em `docs/metodologia-composicao-investimento.md`.
- ✅ **T09** Plano de descontinuação `bruno-dashboard` Streamlit em `docs/plano-descontinuacao-streamlit.md` (4 etapas A-paridade · B-swap · C-pause · D-remoção).
- ✅ **T10** `dashboard/prisma-regional/` → `archive/prisma-regional-design/` com README mapeando port.
- ✅ **T12** Cache HTTP do painel — versionado como `painel.{hash}.json` (1y immutable) + `painel-index.json` (no-store). Reduz transferência de 850KB→<200B em recargas.
- ✅ **T14** `CLAUDE.md` inicial — stack, comandos, convenções, decisões Paulo.
- ✅ **T15** Testes mínimos — 5 pytest (smoke, integridade regiões, regressão `agregar_para_regiao`) + 19 Vitest (format, regioes, integridade tile-grid). Quality gate adaptado em `scripts/quality-gate.sh`.
- ✅ **Coletor populacional IBGE** (SIDRA 6579) — habilita Per capita. Integrado ao painel.
- ✅ **SEFAZ-CE Cota-Parte ICMS/IPVA via SICONFI Anexo 03** — substitui adapter manual bloqueado por bot. Pronto para rodar (~10 min, 2k requests).
- ✅ **MapLegend + MapTooltip + "Por zona"** no aside da Tela 1 (fidelidade ao design original `screen01.jsx`).
- ✅ **Layout responsivo** — `minmax(260px, 320px)` nas colunas, `Panel` com `overflow:auto`, Choropleth com aspect-ratio nativo. Removido clipping silencioso em viewports <1440px.
- ✅ **Composição com 4 esferas** — Estadual (SIOF) + Federal (RREO) + Municipal (SICONFI) + Privado residual (`inv_total - estadual - federal - municipal`).
- ✅ **PERIODO_FIM_MENSAL = 2026** — painel agora cobre 14 regiões × 144 meses (2016 linhas), incluindo o ano corrente do SIOF SEPLAG.

## Em curso (autônomo)

- ⏳ **T01** Coletor SICONFI invest_municipal rodando no Mac Mini (PID 82781). 31% às 23:38 BRT (625/2024 mun-ano), ETA ~04:30 BRT. Quando terminar, `run_coletas.sh` encadeia `--painel-ce` e regenera o painel completo. ScheduleWakeup colhe e faz deploy automático.

## Aguardando terceiros / decisões

- ⏸ **T03** TLS Let's Encrypt em `prisma.bruno.ciciatech.cloud` — emitido na primeira request via DNS público real (já confirmado em produção, mas worth-revalidar).
- ⏸ **T05 (parte 2)** Especificação econométrica multivariada — Prof. Paulo precisa definir variáveis de controle, defasagens (lag), transformações (log/diff), tratamento de endogeneidade (IV / Arellano-Bond), teste de Granger.
- ⏸ **T07** Auto-regenerar painel.json após coletas — exige hook local no Mac Mini que comita o painel + dispara `python3 frontend/scripts/build-data.py` + push em dev. Requer setup adicional ou cron local.
- ⏸ **T11** shadcn/ui — em **WAITING**: sem trigger justificando o refator de tokens (zero forms/dialogs hoje). Re-abrir quando aparecer Dialog/Combobox/DataTable.

## Pendente

- 🟡 **Deflator IPCA → R$ dez/2024** — harmoniza bases monetárias de SIOF (correntes), invest_municipal (correntes), invest_federal (correntes) e FBCF (R$ 2010). Requer coleta de IPCA mensal (BACEN SGS 433) + aplicação como deflator. ~2h.
- 🟡 **Cleanup do repositório** — ~30 arquivos untracked (`analysis/`, `notebook/`, `status.md`, `docs/pdf/`). Decisão manual por arquivo.
- 🟡 **Tests E2E + Lighthouse CI** — automatizar QA visual no pipeline (Playwright + Lighthouse).

## Bloqueado por terceiros

- 🚧 **T16 ESTBAN BNB** — BCB removeu URL pública estável. Caminhos: download manual (132 arquivos, ~2h cliques), Selenium scraper (risco), LAI institucional (Bruno tem vínculo UFC). Adapter local pronto em `dados_nordeste/raw/estban/`.
- 🚧 **T17 SEFAZ-CE adapter manual** — **destravado parcialmente** via SICONFI Anexo 03 (cota-parte ICMS/IPVA). Adapter manual fica como fallback se SICONFI falhar.

---

**Total**: 26 itens registrados (15 ✅ done · 1 ⏳ em curso · 4 ⏸ aguardando · 3 🟡 pendente · 2 🚧 bloqueado · 1 destravado parcialmente).

# Tasks · Prisma Regional / Tese DESP-UFC

Roadmap de tarefas. Sincronizado com `.claude/task-pilot/tasks.md`.

> ✅ done · ⏳ em curso · ⏸ aguardando · 🚧 bloqueado por terceiros · 🔴 urgente

> Histórico detalhado da sprint inicial em `docs/changelog.md`.

---

## 🔴 BLOQUEADOR CIENTÍFICO (descoberto 2026-05-01)

- 🔴 **SCI-01 SIOF regional ausente em 2015-2025** — SEPLAG-CE só publica detalhamento por região (14 macrorregiões) **a partir de 2026** no SIOF-Web. Anos 2015-2025 só têm nível "secretaria" (8 dígitos) sem desagregação regional. Isso **inviabiliza o exercício causal regional 2015-2025** da tese (regressão emprego × investimento estadual em obras nas 14 regiões). **Mitigação UX implementada** (commit pendente): overlay no mapa coroplético quando período sem dado, botão "Ver 2026", nota editorial. **Decisão pendente do Prof. Paulo + Bruno:** (a) restringir tese só a 2026 [pouco dado], (b) requisição LAI à SEPLAG-CE pelos dados regionais 2015-2025, (c) contato direto IPECE/SEPLAG via vínculo UFC do Bruno, (d) pivotar variável de impacto para outro proxy (ex.: invest. municipal SICONFI por região, que tem 180/184 munic. cobertos 2015-2025). Documentado em `docs/metodologia-composicao-investimento.md`.

## 🔴 URGENTE — segurança (descoberto 2026-05-01)

- 🔴 **SEC-01** Rotacionar token Coolify `3|Oq2dlr3X...` — vazado em `.claude/rules/coolify-deploy.md:38` desde commit `f9d813d` em **repo público** `ciciatech/projeto_bruno`. Painel: `painel.ciciacademy.com.br` → API tokens → revogar antigo + gerar novo. Atualizar consumidores: `.mcp.json`, `~/.coolify-tokens`, secret `COOLIFY_TOKEN` no GitHub (usado por `.github/workflows/deploy-coolify.yml`).
- 🔴 **SEC-02** Remover token hardcoded de `.claude/rules/coolify-deploy.md:38` — substituir por placeholder `${COOLIFY_TOKEN}` ou referência ao keychain. Bloqueado por SEC-01 (rotacionar antes pra não quebrar deploy ativo).
- 🔴 **SEC-03** Decidir sobre token Hostinger `zaNo8Tk...` — não vazou no repo, mas estava em arquivo plano `notas.txt` (já gitignored). Opções: (a) mover pro keychain via `security add-generic-password -s "ciciatech-hostinger-api"` e deletar `notas.txt`, (b) rotacionar por garantia.
- 🟡 **SEC-04** (opcional) `git filter-repo` pra remover token do histórico do repo público — só mitiga risco residual a quem clonou; SEC-01 já mata o risco operacional.

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
- ✅ **T01 Coletor SICONFI invest_municipal** — terminou no Mac Mini em 2026-05-01 03:57 BRT (6h25min, 19.896 registros, 180 municípios CE, R$ 28,97 bi 2015-2025, 1,0% suspeitos auditados). Painel reconstruído (1.848 linhas). Substitui planilha manual do Bruno.
- ✅ **Sincronização Mac Mini ↔ local + cleanup do repo (2026-05-01)** — auto-pull travado há 13h por estado sujo no Mac Mini destravado: rsync trouxe outputs de 6h25 de coleta, `git stash -u` preservativo no Mac Mini, pull. Local: removido `python3.13` (binário acidental), `status.md` stale, lock LibreOffice. Outputs organizados em 3 commits temáticos (`eba9dda` data · `de0ce21` docs · `f97233f` chore-quality-gate) pushados pra `dev`.
- ✅ **`.gitignore`: anexo de 463MB** — `docs/parque_infra_ce/dados/Investimento Governo Federal 2014 - 2025.xlsx` excluído (excede limite 100MB do GitHub). Mantido localmente.
- ✅ **Plugar `sefaz_ce_siconfi` no painel (2026-05-01)** — validação end-to-end: painel agora tem 35 colunas (era 31, +`transf_est_icms/ipva/total`) × 2016 linhas. Frontend regenerado (`painel.2de8b30f.json`, 947KB). Test smoke robustecido (calcula `n_meses` dinamicamente + asserção do schema sefaz). Outputs municipais (BPC/BF/CAGED/invest_federal) recuperados do stash do Mac Mini e commitados. Stash drop. Commits `ae8ad05` + `6a47f22`.
- ✅ **Drop stash do Mac Mini** — `auto-stash 2026-05-01` removido após confirmar que todos outputs foram commitados em `ae8ad05`.
- ✅ **Overlay SIOF sem-dado-regional + descoberta SCI-01** — bug aparente no mapa coroplético virou descoberta de bloqueador científico (SEPLAG-CE só publica detalhamento regional a partir de 2026). Mitigação UX: overlay editorial + botão "Ver 2026" + nota explicativa. Bloqueador científico documentado em `docs/metodologia-composicao-investimento.md` e `tasks.md` SCI-01. Commit `e3c6951`, deployado em `prisma.bruno.ciciatech.cloud`.
- ✅ **Remover job `bruno-dashboard` do workflow Coolify** — Streamlit em descontinuação (ver `docs/plano-descontinuacao-streamlit.md` + memory). Job removido de `.github/workflows/deploy-coolify.yml`; só `prisma-frontend` deploya. Streamlit congelado em `e3c6951`. Para redeploy emergencial: curl manual com UUID `p4c0o8wkcgos8s0sscws8g8k` (instrução nos comentários do YAML).

## Em curso (autônomo)

_(nenhuma coleta autônoma rodando no momento)_

## Aguardando terceiros / decisões

- ⏸ **T03** TLS Let's Encrypt em `prisma.bruno.ciciatech.cloud` — emitido na primeira request via DNS público real (já confirmado em produção, mas worth-revalidar).
- ⏸ **T05 (parte 2)** Especificação econométrica multivariada — Prof. Paulo precisa definir variáveis de controle, defasagens (lag), transformações (log/diff), tratamento de endogeneidade (IV / Arellano-Bond), teste de Granger.
- ⏸ **T07** Auto-regenerar painel.json após coletas — exige hook local no Mac Mini que comita o painel + dispara `python3 frontend/scripts/build-data.py` + push em dev. Requer setup adicional ou cron local.
- ⏸ **T11** shadcn/ui — em **WAITING**: sem trigger justificando o refator de tokens (zero forms/dialogs hoje). Re-abrir quando aparecer Dialog/Combobox/DataTable.

## Pendente

- 🟡 **Resolver warning SIOF "Estado do Ceará"** — linha agregada do PDF SIOF SEPLAG sem código regional. Filtrar antes do `agregar_para_regiao` ou mapear como rateio nas 14 regiões. ~30min.
- 🟡 **Lint frontend (4 erros pre-existentes)** — `FilterBar.tsx:25,30,54` (react-refresh/only-export-components) + `vite.config.ts:1` (triple-slash-reference). Não-bloqueante, mas suja `npm run lint`. Mover constantes/helpers de `FilterBar.tsx` pra arquivo separado e migrar `vite.config.ts` pra `import` style. ~30min.
- 🟡 **Deflator IPCA → R$ dez/2024** — harmoniza bases monetárias de SIOF (correntes), invest_municipal (correntes), invest_federal (correntes) e FBCF (R$ 2010). Requer coleta de IPCA mensal (BACEN SGS 433) + aplicação como deflator. ~2h.
- 🟡 **Tests E2E + Lighthouse CI** — automatizar QA visual no pipeline (Playwright + Lighthouse).

## Bloqueado por terceiros

- 🚧 **T16 ESTBAN BNB** — BCB removeu URL pública estável. Caminhos: download manual (132 arquivos, ~2h cliques), Selenium scraper (risco), LAI institucional (Bruno tem vínculo UFC). Adapter local pronto em `dados_nordeste/raw/estban/`.
- 🚧 **T17 SEFAZ-CE adapter manual** — **destravado parcialmente** via SICONFI Anexo 03 (cota-parte ICMS/IPVA). Adapter manual fica como fallback se SICONFI falhar.

---

**Total**: 31 itens registrados (20 ✅ done · 0 ⏳ em curso · 4 ⏸ aguardando · 4 🟡 pendente · 2 🚧 bloqueado · 1 destravado parcialmente · **3 🔴 urgente segurança + 1 🟡 opcional segurança**).

**Última atualização**: 2026-05-01 (sessão 2) — sprint sefaz_ce_siconfi end-to-end + recuperação de outputs municipais + drop stash Mac Mini.

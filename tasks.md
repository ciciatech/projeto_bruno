---
nome: Doutorado Bruno — Investimento Público e Emprego no CE
categoria: academico
status: em_andamento
atualizado_em: 2026-06-11
cliente: Bruno Cardoso / Prof. Paulo Matos (CAEN/UFC)
prioridade: P1
repo: ciciatech/projeto_bruno
producao: https://bruno.ciciatech.cloud
---

# Doutorado Bruno — Investimento Público e Emprego no Ceará

> Legenda: [ ] pendente · [~] em andamento · [x] concluído

Pipeline de coleta + dashboard regional (14 regiões SEPLAG/IPECE do CE) para o
doutorado de Bruno Cardoso (orientador Prof. Paulo Matos, CAEN/UFC). Codinome
interno do frontend: Prisma Regional (`prisma-frontend` no Coolify). Histórico
detalhado da sprint inicial em `docs/changelog.md`. O snapshot
`.claude/task-pilot/tasks.md` (sprint abr/2026) está defasado — este arquivo é a
fonte de verdade do roadmap.

## Urgente — Bloqueadores e segurança

### 🔴 Incidente de produção (11/06/2026) — VPS suspensa

- [ ] (P0) [INFRA] INC-01 **`bruno.ciciatech.cloud` FORA DO AR** — a Hostinger suspendeu a VPS inteira (srv1068785 / 72.60.152.227) e o domínio `ciciatech.cloud` por detecção automática de phishing. **Não é problema de código** — o último deploy (`painel.bcbca74b.json`) subiu OK e está congelado; é suspensão de infra. Gatilho = **falso-positivo** em `pagbankjcln.ciciatech.cloud` (projeto Cazuzua/PagBank, subdomínio vizinho na mesma VPS — sem relação com o Bruno). Recurso aberto com humano da Hostinger; remediação (renomear subdomínio + auth) preparada no repo do Cazuzua. Quando a VPS voltar: confirmar que `bruno.ciciatech.cloud` serve `bcbca74b` (deploy automático/Coolify reativa sozinho). Bruno é **estático** (React/Vite + `painel.json` no repo) — se a suspensão arrastar, dá para subir em host estático (Cloudflare Pages/Netlify) em minutos, num link provisório, sem depender da VPS.
  - Risco lateral confirmado: **não há backups off-site** (`infraestrutura/.../blueprint.md:130`) — dados de clientes presos nos volumes da VPS. Bruno não tem DB, então sem risco de dado para a tese.

### Bloqueador científico

- [x] (P0) [DATA] SCI-01 **DESTRAVADO em 10/06 (noite)** — a premissa "SIOF regional só a partir de 2026" era FALSA: artefato do coletor antigo, que só usava a família de relatórios "Secretaria" (19 de 132 relatórios; o rel. 120 ainda ignora silenciosamente o filtro de elemento). O **relatório 544 ("Região", família "Outros")** entrega Região × {Lei, Lei+Créditos, Empenhado, **Pago**} com filtros grupo 44 + elemento 51/52 para **2012-2026**. Série 2015-2025 coletada e validada no spike (`docs/sci01-spike/siof_544_consolidado_2015_2025.csv`, 318 linhas, R$ 13,96 bi pagos em obras). Validação independente: Balanço Geral do Estado (SEFAZ-CE) publica "Investimentos por Região" 2015-2022 em PDF parseável. Investigação completa em `docs/decisao-sci01-siof-regional.md`. **O exercício causal regional 2015-2025 da tese está viável.** Implementação: T45-T47
  - Ressalvas: 2015 usa as 8 macrorregiões antigas (LC 154/2015 — crosswalk via rel. 532 "PA e Região", T47); resíduo não-regionalizado: obras 0,4-2,3%/ano (ok), equipamentos 2,7-24,8%/ano (registrar na tese); região é a menor unidade espacial do SIOF (sem municipal).
  - Overlay UX do mapa ("Ver 2026") fica obsoleto após T45 — remover na integração.

### Segurança (descoberto 2026-05-01)

- [ ] (P0) [INFRA] SEC-01 Rotacionar token Coolify vazado em repo público desde commit `f9d813d` — revogar em painel.ciciacademy.com.br + atualizar `.mcp.json`, `~/.coolify-tokens`, secret `COOLIFY_TOKEN` do GitHub
- [ ] (P0) [INFRA] SEC-02 Remover token hardcoded de `.claude/rules/coolify-deploy.md:38` — substituir por placeholder/keychain (bloqueado por SEC-01)
- [ ] (P0) [INFRA] SEC-03 Token Hostinger em `notas.txt` (gitignored, não vazou) — mover pro keychain e deletar arquivo, ou rotacionar
- [ ] (P3) [INFRA] SEC-04 (opcional) `git filter-repo` para limpar token do histórico do repo público

## Dados do Prof. Paulo — planilha bimestral (10/06/2026)

- [x] (P1) [DATA] T18 Estudo de viabilidade: mapear fontes do painel bimestral e automatizar coletas — **concluído**, ver `docs/estudo-viabilidade-painel-bimestral.md`
  - Recomendação: SEGUIR com ressalvas. 7/14 blocos já cobertos ou adaptação P; gerou T27-T31.
  - **6 confirmações pendentes com o Paulo**: (1) cota-parte ICMS como proxy de "ICMS recolhido" (razão estável ~0,09; obs.: troca de fonte mudou GF×2024 em 1,6-2×, ver T35); (2) shares federais 15,4% regional (vs 14,5% NE de abr/2026); (3) base dez/25 substitui dez/24 (a planilha prova dez/25 — 25b6=1,0); (4) investimento municipal EMPENHADO serve ou precisa ser PAGO (recoleta ~13k requests, T34); (5) ESTBAN só BNB (39 munic.) ou todos os bancos (~118 munic.)? (6) saldo ESTBAN é R$ correntes (confirmar antes de deflacionar).
- [x] (P1) [DATA] T19 Séries mensais nominais de Bolsa Família e BPC geradas — `docs/dados_prof_paulo/Series mensais nominais - Bolsa Familia e BPC - CE.xlsx` (5 abas: Leia-me, BF/BPC municipal, BF/BPC regional; R$ 54,26 bi BF + R$ 34,21 bi BPC). **Falta só enviar ao Paulo (WhatsApp/e-mail)**
- [x] (P1) [DATA] T20 Planilha atualizada com aba "Índice de inflação" recebida (10/06) — `docs/dados_prof_paulo/Dados Regionais - CC SEFAZ e Tese Bruno-2.xlsx`: deflator bimestral 15b1+ (66 obs, base dez/25). A versão "-2" é a cópia canônica
- [x] (P1) [DATA] T21 Tabela "cidades por região" validada contra `pipeline/data/municipios_ce_regioes.csv` — 184/184 municípios, 0 divergências de nome ou código de região. Arquivo em `docs/dados_prof_paulo/Lista de cidades por região.xlsx`
- [ ] (P1) [DATA] T22 Acompanhar entrega do Magno: investimentos municipais por elemento (equipamentos, obras e residual) — delegado pelo Paulo em 10/06; integrar output ao pipeline quando chegar
- [ ] (P1) [DATA] T23 Acompanhar entrega do Paulo Ícaro: dados de rendimento por município/região — delegado pelo Paulo em 10/06; integrar output ao pipeline quando chegar
- [ ] (P2) [BE] T24 Transferência estadual SIOF (célula HH71): despesas correntes Função 08 (Assistência Social), Subfunções 241–244 — programas Mais Infância, Ceará sem Fome e Vale Gás Social
  - Novo recorte do SIOF; pode esbarrar no SCI-01 (sem desagregação regional antes de 2026). Depende do T18.
- [x] (P2) [DATA] T25 Avaliar séries da letter Matos & Araújo (crédito e inadimplência) como insumo do bloco crédito — **concluída via T31** (8 séries coletadas; decisão de entrarem no modelo fica com a especificação econométrica T05.2) — `docs/dados_prof_paulo/On the indebtedness and delinquency decisions of Brazilian households.pdf`
  - Letter (CAEN/UFC) com rule-of-thumb para endividamento/inadimplência das famílias; Tabela 1 lista séries BACEN SGS nacionais facilmente automatizáveis: 29027 (renda disponível), 22110 (consumo), 20570/20606 (estoque crédito livre/direcionado), 21112/21145 (inadimplência), 25462/25493 (juros).
  - Avaliar no T18 se entram como controles/instrumentos do modelo causal junto com SELIC e IBCR-CE; crédito municipal segue via ESTBAN verbete 160 (T16).
- [x] (P3) [DOC] T26 Arquivos do Paulo organizados em `docs/dados_prof_paulo/` (planilha "-2", lista de cidades, letter PDF, export BF/BPC). Restam 2 duplicatas byte-idênticas (MD5 conferido) aguardando deleção manual: `documento_novo.xlsx` (raiz) e `dados_novos/Dados Regionais - CC SEFAZ e Tese Bruno.xlsx`
- [x] (P1) [BE] T27 Camada bimestral do painel — `pipeline/transform/painel_bimestral.py` + `pipeline/data/matriz_regras_regional.csv` (31 colunas classificadas; salário por média ponderada; `sum(min_count=1)` preserva NaN das 5 regiões sem CAGED). Saída: `painel_regional_ce_bimestral.csv` (1008×56, 15b1–26b6, 20 colunas `_real` dez/25) + export xlsx layout Paulo (`dados_nordeste/processed/exports/`). 8 testes pytest. Gate de outliers anula transf_fed 24b5 (erro de unidade na fonte, ver T32)
- [x] (P1) [BE] T28 Deflator bimestral IPCA base dez/25 — `pipeline/transform/deflator.py`: convenção `I(dez/25) ÷ I(último mês do bimestre)` reproduz os 66 valores do Paulo com erro 2×10⁻¹⁵; base pinada e parametrizável (dez/24 pronto se o Paulo preferir); cache offline `pipeline/data/ipca_433_cache.csv`; 8 testes incl. golden test contra fixture
- [x] (P2) [BE] T29 Aba "Instrumentos estaduais" ingerida (10/06) — `pipeline/extract/instrumentos_estaduais_planilha.py` → `dados_nordeste/processed/instrumentos_estaduais/instrumentos_estaduais_ce_bimestral.csv` (66 linhas 15b1-25b6: RP+Previdenciário e DCL em R$ dez/25, SELIC e IBCR-CE como verificação cruzada). 3 testes. Conceito de RP INCLUI previdenciário (difere do RREO — documentado no docstring)
- [ ] (P2) [BE] T30 Estoque de empregos regional — RAIS 31/12 como base + saldo CAGED acumulado. Esforço M; **desbloqueada** (T33 ✓, CAGED 184/184); coordenar com entrega do Paulo Ícaro (T23)
- [x] (P2) [BE] T31 8 séries SGS da letter adicionadas ao `bacen.py` e coletadas (10/06) — colunas `*_letter` em `bacen_sgs_wide.csv` (21 séries, 3.528 registros, 2011-2025; spot-checks ok: inadimplência livre famílias média 5,64%). 4 testes. Fecha também o T25 (avaliação: séries disponíveis como candidatas a controles — entrada no modelo é decisão da especificação econométrica, T05.2)

## Achados da revisão adversarial (workflow 10/06 — upstream do painel)

- [x] (P1) [BE] T32 Outlier transf_fed set/2024 corrigido — causa-raiz: a fonte STN (CKAN resource `..._202410.csv`, nomenclatura deslocada 1 mês) publicou o mês **em centavos sem separador decimal** (0/231k valores com separador vs ~100% nos vizinhos) → inflação de exatamente 100×. Parser ganhou heurística de detecção de centavos (+5 testes); só set/2024 mudou (÷100 exato nas 184 linhas), FPM Fortaleza = R$ 120,8 mi (faixa sã). Gate de outliers do bimestral: 0 flags; 24b5 válido. Colaterais da fonte STN viram T38-T41
- [x] (P1) [BE] T33 CAGED municipal 18/184 — causa-raiz corrigida (commit `d05162c`): normalização 6→7 dígitos concatenava "0" em vez do dígito verificador IBGE. Recoleta FTP MTE concluída em 10/06: 184/184 municípios, 14/14 regiões. **Incidente pós-recoleta corrigido na mesma noite**: 2025-03 perdido por 550 transitório do FTP (WARNING engolido; painel saiu com "135 meses" como sucesso e 25b2 com fluxos ~metade) — mês re-baixado, série contínua 2015-01→2026-04 (136 meses, 23.987 linhas), painéis/xlsx/JSON regenerados, guard de continuidade no coletor (`meses_faltantes_interior` → ERROR) + teste `test_continuidade_mensal_por_fonte` (cobre todas as fontes; lacunas STN documentadas: 2020-11/2022-10/2023-11/2024-11/2025-11 sem publicação upstream → bimestres anulados; 2018-05 e 2021-08 backfillados). Desbloqueia T30
- [ ] (P1) [BE] T34 Investimento municipal: EMPENHADO vs PAGO — extractor usa "DESPESAS EMPENHADAS ATÉ O BIMESTRE"; gabarito do Paulo é pago (+7% a +37% a.a. de divergência, b1 2-3×). Aguarda confirmação do Paulo; se "pago": recoleta SICONFI ~13k requests. Documentado na matriz/Leia-me do xlsx
- [x] (P2) [BE] T35 Coleta completa `sefaz_ce_siconfi` **concluída em 10/06** (~4h): 18.252 linhas, 169/184 municípios, 2015-2025 (cobertura cresce 103→157 munic./ano; faltantes = não-declarantes do Anexo 03). Fortaleza/2024 preservada nos centavos pelo resume. Coletor corrigido antes da rodada (commit `d05162c`): filtro `no_anexo`, anos 2015-2025, checkpoint/resume, 3 testes. **Atenção (reconciliação)**: os 12 valores antigos de GF×2024 mudaram 1,61-1,96× com a troca de fonte (adapter manual → SICONFI Anexo 03) — divergência conceitual a explicar ao Paulo junto com a confirmação do proxy ICMS
- [ ] (P2) [BE] T36 Corrigir grafia "Maciço do Baturité" → "Maciço de Baturité" (forma oficial IPECE, usada pelo Paulo) em `regioes_ce.py`/`municipios_ce_regioes.csv` + regenerar painéis/frontend — quebra joins por nome com fontes externas
- [ ] (P3) [INFRA] T37 `quality-gate.sh`: adicionar `set -o pipefail` (hoje imprime "Frontend OK" com eslint quebrado) e documentar setup local (venv + `npm install` ausentes nesta máquina)
- [ ] (P2) [BE] T38 STN ago/2024 incompleto na fonte atual (resource CKAN de hoje não tem Fortaleza) — manter cache/backup local; recoleta completa PIORARIA ago/2024. Auditar e congelar o mês bom
- [ ] (P2) [BE] T39 Meses de novembro ausentes do painel transf_fed (2020-11, 2022-10, 2023-11, 2024-11, 2025-11) — diagnósticos conflitantes: dedup de resources duplicados no CKAN (`_listar_resources_por_periodo` guarda 1 por chave) vs "sem publicação upstream". Auditar o CKAN e corrigir o dedup se for o caso; bimestres afetados hoje são anulados pelo guard de continuidade
- [ ] (P3) [BE] T40 Auditar out/2024 de transf_fed (R$ 0,73 bi, baixo vs mediana 1,58 bi, formato normal) — real ou incompleto na fonte?
- [ ] (P2) [BE] T41 Risco de dupla contagem em transferencias_municipais: chaves (ano, None) anuais e (ano, mes) mensais coexistem (ex.: 2016) — auditar o merge e deduplicar
- [ ] (P3) [BE] T42 (cosmético) Renomear prefixo duplo `bnb_bnb_*` das colunas ESTBAN no painel (builder prefixa colunas já prefixadas) — tocar extractor/builder/matriz/testes num passo só
- [ ] (P3) [BE] T43 Adicionar aba de crédito BNB (verbete 160) ao xlsx do Paulo (`VARIAVEIS_XLSX` em `painel_bimestral.py`) — trivial; aguarda resposta da pergunta 5 (BNB vs todos os bancos)
- [ ] (P1) [INFRA] T44 Deploy via GitHub Actions quebrado — runners não alcançam `painel.ciciacademy.com.br:443` (timeout 135s; falhou em 3 pushes seguidos em 10/06, painel ok da rede local). Suspeita: firewall da VPS bloqueando ranges do GitHub. Investigar firewall Hostinger/CSF; enquanto isso, deploy manual via curl da máquina local (CLAUDE.md § Deploy)
- [x] (P0) [BE] T45 Coletor `pipeline/extract/siof_regiao.py` **implementado, coletado e integrado (11/06)** — 132/132 relatórios (2015-2025 × 6 bimestres × elementos 51/52), 0 falhas, spike validado 318/318. Descoberta de mecânica: para mês≠12 o servidor descartava o filtro de elemento silenciosamente — corrigido com postback explícito da cascata + validação do rodapé "Critérios" em cada XLS. **TESTE DE OURO vs planilha do Paulo: obras 806/806 células dentro de ±1% (802 dentro de ±0,01%, razão mediana 1,000000); equipamentos 655/656 dentro de ±0,01%** — a variável central da tese está automatizada de ponta a ponta. Painéis: mensal 2016×44 (siof 2016-2026 + split obras/equip), bimestral 1008×80 (fluxos nativos deflacionados), xlsx 13 abas (blocos 4-6 do Paulo completos). Overlay do mapa restrito a 2015 (T47). Cache bruto commitado (2,1MB, reexecução em segundos)
- [ ] (P2) [BE] T46 Parser do Balanço Geral do Estado (PDF, `pdftotext -layout`) — tabela "Investimentos por Região" 2015-2022 como validação independente do T45 (empenhado, GND 4 sem split 51/52). Esforço P
- [ ] (P2) [DATA] T47 Crosswalk 2015: 8 macrorregiões antigas → 14 regiões — via relatório 532 ("PA e Região", nível ação/projeto, descrições citam municípios) ou decisão metodológica com o Paulo (descartar 2015? manter nas 8?). Decide a quebra de série da LC 154/2015

## Aguardando terceiros / decisões

- [ ] (P2) [INFRA] T03 Revalidar TLS Let's Encrypt em prisma.bruno.ciciatech.cloud (já confirmado em produção, worth-revalidar)
- [ ] (P1) [DATA] T05.2 Especificação econométrica multivariada — Prof. Paulo precisa definir controles, lags, transformações (log/diff), endogeneidade (IV/Arellano-Bond), Granger
- [ ] (P2) [INFRA] T07 Auto-regenerar painel.json após coletas — hook local no Mac Mini (commit painel + build-data.py + push dev)
- [ ] (P3) [FE] T11 shadcn/ui — WAITING: sem trigger (zero forms/dialogs hoje); reabrir quando aparecer Dialog/Combobox/DataTable
- [x] (P2) [DATA] T16 ESTBAN **destravado e coletado em 10/06** — o bloqueio estava desatualizado: o BCB voltou a publicar download direto (API JSON lista 453 meses; 132/132 baixados, 139MB, 0 erros). Verbete 160 agregado município×mês em `dados_nordeste/processed/estban/credito_municipio_ce_mensal.csv` (5.148 linhas, 39 munic. com agência BNB, 14 regiões) + detalhe por verbete no companion `credito_bnb_*`. Plugado no painel (7 colunas novas). Bug real corrigido no parse (202209 com CODMUN vazio). 10 testes
  - ⚠️ 2 perguntas novas ao Paulo: (a) a série deve ser **só BNB** (39 munic. com agência) ou **todos os bancos** do ESTBAN (~118 munic.)? — trocar é só reprocessamento local, zips já baixados; (b) confirmar que o saldo é R$ **correntes** antes de deflacionar para dez/25. Lembrete: crédito é registrado no município da **agência**, não do tomador; série disponível desde 1988-07 se quiser estender.
- [ ] (P2) [DATA] T17 SEFAZ-CE adapter manual — destravado parcialmente via SICONFI Anexo 03; adapter manual fica como fallback

## Pendente

- [ ] (P2) [BE] Resolver warning SIOF "Estado do Ceará" — linha agregada sem código regional; filtrar antes do `agregar_para_regiao` ou ratear nas 14 regiões. ~30min
- [x] (P2) [FE] Lint frontend zerado (11/06) — constantes/hooks movidos para `lib/filtros.ts`, `vite.config.ts` migrado de triple-slash; `npm run lint` = 0 erros. Revisão de visualização junto: split obras×equipamentos na Síntese, tooltips de valor exato e de "sem cobertura da fonte", legenda do mapa com faixas no hover, aria-labels/aria-pressed, echo do período na FilterBar, fix de bug de unidade ×1e6 na tela Investimento. Vitest 19→37 testes
- [ ] (P2) [BE] Deflator IPCA do painel mensal — harmonizar bases monetárias SIOF/invest_municipal/invest_federal/FBCF. **Superseded parcialmente pelo T28**: base provável dez/25 (planilha do Paulo), aguarda mesma confirmação
- [ ] (P2) [QA] Tests E2E + Lighthouse CI — automatizar QA visual (Playwright + Lighthouse)

## Concluídas

- [x] (P1) [FE] T02 Tela 2 Emprego com CAGED real — mapa divergente, KPIs, tabela 14 regiões (cobertura 9/14 à época — artefato do bug de normalização corrigido no T33; recoleta em curso)
- [x] (P1) [FE] T04 Tela 3 — pivot para "Composição de Receitas Públicas Regionais" (FPM/FUNDEB/royalties/ITR/outros + BF + BPC)
- [x] (P1) [FE] T05 Tela 4 Causal — OLS univariado real com `simple-statistics` (scatter + IC 95% + β/α/R²/σ); especificação preliminar
- [x] (P1) [FE] T06 FilterBar funcional (período + recorte com URL state via react-router)
- [x] (P1) [DATA] T08 Decisão invest_privado residual — FBCF Brasil mensal × 2,2% share PIB CE/BR, R$ dez/2024 (áudios Paulo abr/2026, ver `docs/metodologia-composicao-investimento.md`; base em revisão → dez/25, ver T28 e confirmações pendentes do T18)
- [x] (P2) [DOC] T09 Plano de descontinuação `bruno-dashboard` Streamlit (`docs/plano-descontinuacao-streamlit.md`, etapas A-D)
- [x] (P3) [DOC] T10 `dashboard/prisma-regional/` → `archive/prisma-regional-design/` com README de port
- [x] (P2) [FE] T12 Cache HTTP do painel — `painel.{hash}.json` (1y immutable) + `painel-index.json` (no-store); 850KB→<200B em recargas
- [x] (P2) [DOC] T14 `CLAUDE.md` inicial — stack, comandos, convenções, decisões Paulo
- [x] (P1) [QA] T15 Testes mínimos — 5 pytest + 19 Vitest + quality gate em `scripts/quality-gate.sh`
- [x] (P2) [BE] Coletor populacional IBGE (SIDRA 6579) — habilita Per capita, integrado ao painel
- [x] (P1) [BE] SEFAZ-CE Cota-Parte ICMS/IPVA via SICONFI Anexo 03 — substitui adapter manual bloqueado por bot
- [x] (P2) [FE] MapLegend + MapTooltip + "Por zona" no aside da Tela 1 (fidelidade ao design original)
- [x] (P2) [FE] Layout responsivo — `minmax(260px,320px)`, `Panel` com overflow, Choropleth aspect-ratio nativo; removido clipping <1440px
- [x] (P1) [BE] Composição com 4 esferas — Estadual (SIOF) + Federal (RREO) + Municipal (SICONFI) + Privado residual
- [x] (P1) [BE] PERIODO_FIM_MENSAL = 2026 — painel cobre 14 regiões × 144 meses (2016 linhas)
- [x] (P0) [BE] T01 Coletor SICONFI invest_municipal — 19.896 registros, 180 municípios CE, R$ 28,97 bi 2015-2025; substitui planilha manual do Bruno
- [x] (P2) [INFRA] Sincronização Mac Mini ↔ local + cleanup do repo (2026-05-01) — rsync de outputs, stash preservativo, 3 commits temáticos
- [x] (P3) [INFRA] `.gitignore`: anexo de 463MB (`Investimento Governo Federal 2014 - 2025.xlsx`) excluído do repo, mantido local
- [x] (P1) [BE] Plugar `sefaz_ce_siconfi` no painel — 35 colunas × 2016 linhas, frontend regenerado, smoke test robustecido (commits `ae8ad05` + `6a47f22`)
- [x] (P3) [INFRA] Drop stash do Mac Mini após confirmar outputs commitados em `ae8ad05`
- [x] (P1) [FE] Overlay SIOF sem-dado-regional + descoberta SCI-01 — mitigação UX deployada (commit `e3c6951`)
- [x] (P2) [INFRA] Remover job `bruno-dashboard` do workflow Coolify — só `prisma-frontend` deploya; Streamlit congelado em `e3c6951`
- [x] (P1) [INFRA] Etapa B-swap da descontinuação Streamlit (2026-05-02) — `bruno.ciciatech.cloud` servido pelo `prisma-frontend` via PATCH fqdn na API Coolify; TLS ok; `prisma.bruno.ciciatech.cloud` mantido como fallback. Restam etapas C-pause + D-remoção

---

**Estado para a retomada (11/06)**: código 100% commitado e pushado — `dev` = `origin/main` = `7c06ee0`, working tree limpo (só restam 2 duplicatas `documento_novo.xlsx` + `dados_novos/` aguardando deleção manual). Produção (`bruno.ciciatech.cloud`) **fora do ar por INC-01** (VPS suspensa — infra, não código; último deploy `bcbca74b` íntegro e congelado). Projeto **registrado no portal CiciaTech** (grupo `orientacao` adicionado ao `gerar_referencia.py`, card "Doutorado Bruno" 41/69 ≈ 59%, commitado no repo do portal `ciciatech-portal@3871e9e`) — só aparece quando a VPS voltar. Próximos passos de código: T45 destravou tudo, então T46 (parser BGE validação), T47 (crosswalk 2015 das 8→14 regiões), T30 (estoque de empregos, desbloqueado). Pendências humanas: 6 confirmações + xlsx BF/BPC ao Paulo (T18/T19), papéis Magno/Matos com o Bruno, SEC-01 (token — urgência redobrada após o incidente).

**Última atualização**: 2026-06-11 — **T45 entregue**: coletor SIOF regional (rel. 544) coletado e integrado; teste de ouro vs planilha do Paulo fecha em 1,000000 (obras 806/806 ±1%, equip 655/656 ±0,01%) — a variável central da tese está automatizada. Frontend revisado (lint 0, 37 testes, tela Investimento acesa 2016+, split obras/equip). Incidente grave corrigido: teste do pytest sobrescrevia o CSV real do CAGED (commit anterior está corrompido; este conserta). Antes disso (madrugada 10/06): **SCI-01 DESTRAVADO**: investigação multi-frente provou que "região só em 2026" era artefato do coletor; o relatório 544 do SIOF entrega Região × Pago para 2012-2026 (spike validado, BGE como contraprova) — o exercício causal 2015-2025 da tese está viável. Abertos T45-T47. Antes disso, dia de virada: T27/T28 (painel bimestral + deflator dez/25 validado em 2×10⁻¹⁵), 6 tasks de dados fechadas (T16 ESTBAN destravado no BCB, T25/T29/T31 instrumentos+séries da letter, T32 set/2024 era centavos na fonte, T33 CAGED 18→184 municípios com recuperação do mar/2025, T35 SEFAZ completa 18,2k linhas), rebranding "Doutorado Bruno — Investimento Público e Emprego no Ceará" aplicado (painel + docs + portal), tela Pipeline com status reais, suíte 73 pytest + 19 vitest. Painel mensal 2016×42, bimestral 1008×70, frontend hash `aeae3233`. Colaterais da fonte STN viram T38-T43. **6 confirmações pendentes com o Prof. Paulo** (ver T18) + enviar xlsx BF/BPC (T19).

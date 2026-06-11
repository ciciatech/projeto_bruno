# Estudo de viabilidade — Painel bimestral do Prof. Paulo (T18)

**Data**: 2026-06-10 (revisado na mesma data após verificação multi-agente — ver Adendo) · **Trilho**: viabilidade · **Insumos**: `docs/dados_prof_paulo/Dados Regionais - CC SEFAZ e Tese Bruno-2.xlsx` (3 abas), esclarecimentos WhatsApp 10/06, letter Matos & Araújo (crédito/inadimplência), inventário do pipeline.

> **Errata**: a primeira versão deste estudo afirmava que "não há agregação bimestral no transform" e que RP/DCL "não são coletados". Ambas estavam **erradas** — a exploração inicial não viu `pipeline/transform/preparacao_modelo.py` (pipeline bimestral legado NE×UF, com deflator dez/25 e coletores RREO/RGF cobrindo o CE). As seções abaixo foram corrigidas e o Adendo registra o que foi implementado.

## Pergunta

Para cada bloco da planilha bimestral do Prof. Paulo (14 regiões, R$ dez/25): qual a fonte exata, dá para automatizar a coleta no pipeline atual, e o que falta para gerar o painel bimestral no formato dele?

## Critérios de viabilidade

Fonte pública estável (API/bulk) · encaixe no pipeline existente (`safe_request`, `agregar_para_regiao`, painel canônico) · granularidade regional 14 SEPLAG ou municipal agregável · cobertura 2015+.

## Mapeamento fonte → coletor (aba "Dados regionais")

| # | Bloco da planilha | Fonte | Coletor no pipeline | Status | Esforço |
|---|---|---|---|---|---|
| 1 | Investimento municipal pago | SICONFI RREO Anexo 01 | `invest_municipal_siconfi.py` | ✅ **já temos** — dado nativo é bimestral (hoje mensalizado por interpolação; para o painel do Paulo usar o bimestral bruto) | P |
| 2 | ICMS recolhido | SEFAZ | `sefaz_ce_siconfi.py` (cota-parte ICMS/IPVA via Anexo 03) | ⚠️ **adaptar/confirmar** — temos **cota-parte transferida aos municípios**; a planilha diz "ICMS recolhido pelos municípios". Confirmar com Paulo se cota-parte serve como proxy regional | P (se cota-parte serve) |
| 3 | Estoque de empregos (RAIS+CAGED) | FTP PDET/MTE | `caged_rais.py` (RAIS estoque anual) + `caged_municipal.py` (fluxo mensal) | 🔧 **adaptar** — construir estoque mensal: RAIS 31/12 como base + saldo CAGED acumulado | M |
| 4-6 | Invest. estadual equipamentos / obras / total | SIOF (grupo 44, elementos 51/52) | `siof.py` (já extrai elemento 51 obras e 52 equipamentos, com filtro por região) | 🚧 **bloqueado parcial (SCI-01)** — sem desagregação regional 2015-2025; 2026+ ok | P (2026+) |
| 7-8 | Invest. União não identificado / identificado | RREO federal | `invest_federal.py` (direto + NE×14,5% + nacional×2,2%) | ⚠️ **adaptar/confirmar** — planilha usa share **REGIONAL 15,4% CE** (CLAUDE.md registra 14,5% NE) + vetor de pesos regionais embutido no cabeçalho do bloco; confirmar revisão dos shares | P |
| 9 | Rendimento formal (CLT+estatutário) | CAGED | `caged_municipal.py` tem só **salário médio do fluxo** | 🔧 **adaptar / delegado** — massa salarial total é a bola do **Paulo Ícaro** (T23); integrar quando chegar | M (ou terceiro) |
| 10 | Residual famílias/firmas | IPEADATA (FBCF) | `ipea_fbcf.py` + share PIB | ⚠️ **adaptar** — residual = invest_total − estadual − federal − municipal (já no frontend); pendência: extrapolação FBCF 2025/26 (IpeaData não publicou) | M |
| 11 | Crédito (verbete 160 — operações de crédito, por município) | ESTBAN/BCB | `estban.py` (adapter pronto) | 🚧 **bloqueado (T16)** — BCB removeu URL pública; caminhos: download manual (132 arquivos), scraper, LAI. Verbete confirmado pelo Paulo | M (após destravar) |
| 12 | Bolsa Família | Portal da Transparência | `bolsa_familia.py` | ✅ **já temos** (2015+, nominal, municipal) — export enviado ao Paulo (T19) | — |
| 13 | BPC | Portal da Transparência | `bpc.py` | ✅ **já temos** (2019+) | — |
| 14 | Transferência estadual (HH71: Função 08, Subfunções 241-244 — Mais Infância, Ceará sem Fome, Vale Gás) | SIOF | `siof.py` já suporta filtros função/subfunção nos WebForms | 🔧 **novo recorte, infra pronta** — mesma limitação SCI-01 para regional <2026 | P/M |

## Aba "Instrumentos estaduais"

| Variável | Fonte | Status | Esforço |
|---|---|---|---|
| Resultado Primário + Previdenciário (CE) | SICONFI RREO (já coletado em `preparacao_modelo.py` → `resultado_primario_bimestral.csv`, CE incluso) | ⚠️ **parcial** — CSV só tem 2023+ e **sem previdenciário** (sinais opostos à aba em 12/18 períodos). Recomendação: ingerir a aba do Paulo como série canônica 2015-2025 | P |
| DCL — Dívida Consolidada Líquida (CE) | SICONFI RGF (já coletada → `dcl_bimestral.csv`, 0% nulos) | ⚠️ **parcial** — bate <1% com a aba em b2/b4/b6 (fins de quadrimestre); nos demais o CSV repete o quadrimestre e diverge 7-11%. Aba do Paulo tem DCL bimestral efetiva | P |
| SELIC | BACEN SGS 4189 | ✅ já coletada (`bacen.py`) — só não entra no painel | P |
| IBCR-CE | BACEN SGS 25380 | ✅ já no painel | — |

## Aba "Índice de inflação"

Deflator bimestral base dez/25 (66 obs, 15b1+). **Convenção decifrada e validada com precisão de máquina** (erro relativo máx 2×10⁻¹⁵ nas 66 obs): `deflator(b) = índice IPCA acumulado de dez/2025 ÷ índice do último mês do bimestre` (b1 usa fev, ..., b6 usa dez), índice via `cumprod(1+ipca/100)` do SGS 433. Convenções alternativas (primeiro mês, médias) erram 0,5–1,35%. O pipeline legado (`preparacao_modelo.py`) já usa **exatamente a mesma convenção**, só com base rolante (`iloc[-1]`) em vez de pinada. ⚠️ Conflito de base dez/24 (decisão abr/2026) vs dez/25: a planilha prova empiricamente que a base do Paulo é dez/25 (25b6 = 1,0 exato) — a confirmação vira formalidade. Implementado em `pipeline/transform/deflator.py` com base parametrizável (default dez/25).

## Séries da letter (Matos & Araújo) — candidatas a controles

8 séries SGS nacionais (29027 renda, 22110 consumo, 20570/20606 estoques de crédito, 21112/21145 inadimplência, 25462/25493 juros) — `bacen.py` já tem o padrão; adicionar é trivial (P). Decisão de incluí-las como controles/instrumentos é da especificação econométrica (T05.2, com o Paulo).

## Transversal — agregação bimestral

**Correção**: o transform legado já tinha `agregar_mensal_para_bimestral` (`preparacao_modelo.py:250-285`) com a semântica fluxo/estoque/índice — mas para o painel **regional** foi necessário um módulo novo (`transform/painel_bimestral.py`, T27) porque o `.agg('sum')` do legado zera grupos 100% NaN e corromperia as 5 regiões sem CAGED (usamos `sum(min_count=1)` com teste de regressão). A matriz de regras regional (`pipeline/data/matriz_regras_regional.csv`) é lida em runtime (fonte de verdade, não dump) e cobre as 31 colunas de valor.

## Decisão

**Recomendação: SEGUIR (com ressalvas).** 7 dos 14 blocos já estão cobertos ou exigem adaptação P; os novos coletores (RP/DCL, transferência estadual F08) usam infra existente (SICONFI genérico, SIOF WebForm). Ressalvas:

1. **SCI-01 segue sendo o risco dominante** — tudo que depende de SIOF regional 2015-2025 (blocos 4-6, 14) fica restrito a 2026+ até decisão LAI/IPECE.
2. **ESTBAN (T16)** continua bloqueado por acesso, não por engenharia.
3. **4 confirmações com o Paulo**: (a) cota-parte ICMS serve como proxy de "ICMS recolhido"? (b) shares federais revisados para 15,4% regional? (c) base dez/25 substitui dez/24? (d) investimento municipal EMPENHADO serve, ou precisa recoletar PAGO (T34, ver Adendo)?
4. Blocos 3 e 9 (estoque emprego, rendimento) dependem em parte da entrega do **Paulo Ícaro** (T23); bloco de invest. municipal por elemento é do **Magno** (T22).

## Próximos passos (issues de implementação)

- **T27** (P1, M) Camada bimestral do painel + export xlsx no layout do Paulo. ✅ **implementada** (ver Adendo)
- **T28** (P1, P) Deflator bimestral IPCA base dez/25 + validação contra a aba "Índice de inflação". ✅ **implementada** (ver Adendo)
- **T29** (P2, **P** — redimensionada) Ingerir a aba "Instrumentos estaduais" como série canônica de RP+Previdenciário e DCL do CE 2015-2025, documentando a diferença conceitual com o RREO (CSV legado: RP só 2023+ sem previdenciário; DCL repete quadrimestre).
- **T30** (P2, M) Estoque de empregos regional (RAIS base + CAGED acumulado) — **bloqueada pelo T33** (cobertura CAGED municipal).
- **T31** (P2, P) Séries SGS da letter no `bacen.py` (executa o T25).
- **T24** (P2, P/M) Transferência estadual SIOF Função 08/241-244 (já aberta).

---

## Adendo — implementação T27/T28 e achados da revisão adversarial (2026-06-10)

Workflow multi-agente (3 verificadores → 2 implementadores → 3 revisores adversariais → 1 corretor → 1 gate). Resultado: **28/28 testes pytest verdes**, cobertura 24% (gate ≥10%).

### Entregue

- `pipeline/transform/deflator.py` (T28) — base **pinada e parametrizável** (default `2025-12`, corrige a base rolante do legado); reproduz os 66 deflatores do Paulo com erro máx 1,97×10⁻¹⁵; cache offline `pipeline/data/ipca_433_cache.csv`; 8 testes.
- `pipeline/transform/painel_bimestral.py` + `pipeline/data/matriz_regras_regional.csv` (T27) — painel `dados_nordeste/processed/model_ready/painel_regional_ce_bimestral.csv` (1008 linhas = 14 regiões × 72 bimestres 15b1–26b6, 56 colunas, 20 `*_real` dez/25) + export `dados_nordeste/processed/exports/painel_regional_ce_bimestral.xlsx` (Leia-me + 9 abas no layout do Paulo); salário por média ponderada; anuais-replicados por último mês; 8 testes.
- Gate de qualidade na camada: outlier de transferências federais de **set/2024** (erro de unidade na fonte STN; **~86× no agregado regional bimestral** — no dado de origem, FPM municipal de Fortaleza R$ 12,08 bi vs ~R$ 75 mi típicos, **~161×**, ver T32) detectado e anulado de forma visível (24b5 vazio no xlsx); registro em `dados_nordeste/quality/painel_regional_ce_outliers.csv`.

### Achados graves da revisão (upstream, viram tasks)

1. **Outlier transf_fed set/2024** — FPM de Fortaleza R$ 12,08 bi (vs ~R$ 75 mi nos vizinhos). Mitigado na camada T27; a fonte (`transf_constitucionais_ce_mensal.csv`) e o painel mensal continuam com o valor cru → recoletar (T32).
2. **CAGED municipal cobre só 18 de 184 municípios** — o "saldo" regional é ~6% da variação de estoque do gabarito do Paulo (provável bug no merge município→região, não falta de dado na fonte). Disclaimers adicionados ao Leia-me/matriz → investigar e reprocessar (T33). Reclassifica o bloco "Estoque de empregos" do mapeamento de "adaptar" para **bloqueado por T33**.
3. **invest_mun é EMPENHADO, o gabarito é PAGO** — divergência sistemática +7% a +37% a.a. (b1 com razão 2-3×, b6 0,6-0,75×). Documentado na matriz/Leia-me; trocar para "pago" exige recoleta SICONFI (~13 mil requests) → **4ª confirmação com o Paulo** + T34.
4. **transf_est_* só existe para Grande Fortaleza × 2024** — a coleta `sefaz_ce_siconfi` nunca rodou completa → T35.
5. **Grafia "Maciço do Baturité"** (pipeline) vs "Maciço de Baturité" (Paulo/IPECE oficial) — quebra joins por nome → T36.

### Atualização — fim do dia 10/06 (segunda rodada de execução)

Status dos bloqueios mapeados acima, após as coletas da noite: **bloco 3 (estoque de empregos) desbloqueado** — CAGED recoletado com 184/184 municípios (bug do dígito verificador, T33; incidente mar/2025 recuperado na mesma noite); **bloco 11 (crédito/ESTBAN) destravado** — o BCB voltou a publicar download direto, 132/132 meses coletados (T16; pendem 2 confirmações com o Paulo: só BNB vs todos os bancos, e unidade); **bloco 2 (ICMS) coletado por completo** via SICONFI Anexo 03 (T35, 18.252 linhas — valores antigos de GF×2024 mudaram 1,6-2× com a troca de fonte, reconciliar com o Paulo); **instrumentos estaduais ingeridos** da aba do orientador (T29) e **séries da letter coletadas** (T31). set/2024 das transferências era erro de unidade **da fonte** (centavos sem separador, T32) — corrigido com heurística no parser. Painel mensal final: 2016×42; bimestral 1008×70; confirmações com o Paulo agora são **6** (ver tasks.md T18).

### Validações que PASSARAM na tentativa de refutação

Convenção do deflator (66/66), ausência de off-by-one nos `*_real`, bordas de ano íntegras (somas anuais mensal == bimestral), média ponderada exata nos 593 grupos, xlsx == CSV (924/924 células), NaN estrutural preservado. BF/BPC/crédito/rendimento **não têm contraprova** — os blocos correspondentes da planilha do Paulo estão vazios (são justamente o que ele nos pediu).

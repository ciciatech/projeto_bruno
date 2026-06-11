# SCI-01 — Documento de decisão: investimento estadual executado (51/52) por região, 2015-2025

**Data:** 2026-06-10 · **Sintetizado a partir de 5 frentes de investigação** · **Repo:** `/Users/cassiopinheiro/dev/cicia_tech/orientacao/projeto_bruno`

## TL;DR

A premissa que originou o SCI-01 — "o SIOF-Web só regionaliza a partir de 2026" — **é falsa**. Era um artefato do coletor (`pipeline/extract/siof.py` usa apenas a família de relatórios "Secretaria"; o relatório 120 dessa família não traz coluna de região **e ignora silenciosamente o filtro de elemento**). O SIOF-Web tem 5 famílias e 132 relatórios; o **relatório 544 ("Região", família "Outros")** entrega exatamente o que a tese precisa: **Região × {Lei, Lei+Créditos, Empenhado, Pago}, filtrável por grupo 44 + elemento 51 (Obras) ou 52 (Equipamentos), para todos os anos 2012-2026**. A série 2015-2025 completa **já foi coletada e validada** durante a investigação (22 XLS + consolidado em `/tmp/siof_544_consolidado_2015_2025.csv`, filtros confirmados no rodapé "Critérios:" de cada arquivo).

**Status proposto: SCI-01 bloqueado → DESTRAVADO** (com dois caveats metodológicos a registrar na tese, não bloqueadores: quebra de regionalização em 2015 e resíduo não-regionalizado de equipamentos).

---

## 1. Ranking dos caminhos

Critérios: cobertura temporal × granularidade espacial × estágio da despesa × split 51/52 × esforço × risco.

| # | Caminho | Cobertura | Granularidade | Estágio | Split 51/52 | Esforço | Risco | Veredito |
|---|---------|-----------|---------------|---------|-------------|---------|-------|----------|
| **1** | **SIOF rel. 544 (+532)** | **2012-2026** (toda a janela da tese) | 14 regiões (2016+); 8 macrorregiões antigas em 2015 | **Lei, Lei+Créd, Empenhado, Pago** | **Sim, nativo** | **P** (rede já provada em script descartável) | Baixo (1 timeout 504 em ~35 req., sem WAF, sem __EVENTVALIDATION) | **Resolve o SCI-01** |
| 2 | BGE/SEFAZ (PDFs) | 2015-2022 (descontinuado em 2023) | 14 regiões (2016-2022); 8 em 2015 | Autorizado + Empenhado (sem Pago) | Não (GND 4 agregado) | P (meio dia, `pdftotext -layout` testado) | Quase nulo (URLs estáveis) | Validação independente |
| 3 | Obras xlsx + join SACC→API aberta | 2010-2025 | **Município** (94,2% das linhas, 184 municípios → 14 regiões via `regioes_ce.py`) | Contratado total + empenhado/pago **acumulados** por contrato (join 8/8 validado) | Só obras (51); 52 ausente | M (1-2 dias, ~4.962 GETs com throttle) | Baixo (API sem WAF); limitação metodológica: série anual exige **rateio temporal** | Extensão municipal p/ obras |
| 4 | Ceará Transparente `/investimentos-macrorregioes` e `/investimentos-municipio` | 2015-2022 confirmado (provável até 2025) | 14 regiões + município/MAPP | Empenhado + Pago | Não (só por nome do MAPP) | M-G (Playwright browser real p/ vencer AWS WAF; headless puro → 403) | **Alto** (WAF ativo e seletivo; exatamente o scraping que o `sefaz_ce.py` quis evitar) | Redundante agora que o 544 funciona |
| 5 | API aberta CT — convênios | 2015-2025 | Município (via nome do credor) | Empenhado + Pago | Não (inferência por regex no objeto) | P-M (~1 dia) | Baixo (sem WAF), mas **proxy ruim**: só convênios estado→município, não captura execução direta (DER/SEINFRA); data_assinatura ≠ exercício | Descartar como fonte primária |

Fontes confirmadas e descartadas para o regional: IPECE Ceará em Números (só estadual), SICONFI DCA (só estadual — manter como **total de controle**), TCE-CE, MAPP/WebMAPP (interno, sem face pública), `dadosabertos.ce.gov.br` (não existe).

---

## 2. O que construir (top do ranking)

### 2.1 Coletor SIOF-Região — **prioridade 1, esforço P (0,5-1 dia)**

Módulo **novo** (ex.: `pipeline/extract/siof_regiao.py`), **sem tocar** o `siof.py` existente:

1. **Fluxo de rede (já provado em `/tmp/siof_test.py`):** GET `frm_consulta_execucao.aspx` → POST de troca de família (`__EVENTTARGET=ctl00$cphCorpo$rblRelatorio$3`, `rblRelatorio="Outros"`) → POST `btnVisualizar` com `ddlRelatorio=544`, `ddlDespGrupo=44`, `ddlDespElemento ∈ {201290 (51-Obras), 137 (52-Equip)}`, formato `Xlss` → GET `/siofconsulta/Exports/rel_*.XLS`. Delays de 2,5-3s, retry para 504.
2. **Parser dual-esquema:** códigos 01-08 + 22 "Estado do Ceará" até 2015; códigos 01-14 + 15 "Estado do Ceará" de 2016 em diante; extrai Lei / Lei+Créditos / Empenhado / Pago por região.
3. **Cache + validação:** conferir rodapé "Critérios:" (prova de que o filtro de elemento foi aplicado — o rel. 120 NÃO aplica) e total geral por ano.
4. Opcional: coleta anual do **rel. 532 (PA × Região)** como painel de auditoria (ex.: 1.453 linhas para obras-2020).

**Resolve:** a essência completa do SCI-01 — 51 e 52, empenhado **e** pago, 14 regiões, anual (ou mensal-acumulado), 2016-2025; 2015 nas 8 macrorregiões antigas.

### 2.2 Parser BGE — **prioridade 2, esforço P (meio dia)**

Script que baixa os 8 PDFs (URLs estáveis em `ce.gov.br/sefaz`), roda `pdftotext -layout` e extrai a tabela "Investimentos por Região de Planejamento" → CSV tidy (ano, região, autorizado, empenhado), 2015-2022. **Papel:** validação cruzada independente do SIOF (GND 4 ⊇ 51+52; os totais devem conversar). Não substitui o 544 — sem split 51/52, sem Pago, morre em 2022.

### 2.3 Painel municipal de obras (xlsx + SACC) — **prioridade 3, esforço M (1-2 dias), opcional**

ETL sobre `docs/parque_infra_ce/dados/Obras 2008 - 2025.xlsx` (6.790 obras, município em 94,2%) + enriquecimento via `api-dados-abertos.cearatransparente.ce.gov.br/transparencia/contratos/contratos/{sacc}` (sem WAF, join validado 8/8) → empenhado/pago acumulados por obra → agregação município→14 regiões → rateio temporal para o painel anual. **Papel:** robustez e granularidade municipal **para obras (51)** — útil para a tese como análise complementar/heterogeneidade intra-regional, com a limitação do rateio registrada explicitamente. Validar o total anual contra o elemento 51 do SIOF (2.1).

---

## 3. Combinação recomendada

| Camada | Fonte | Entrega |
|--------|-------|---------|
| **Painel principal** | SIOF rel. 544 | região × ano × elemento (51/52) × {empenhado, pago}, 2015-2025 |
| Tratamento de 2015 | SIOF rel. 532 (PA×Região) **ou** crosswalk 14→8 | (a) agregar toda a série às 8 macrorregiões quando 2015 entrar na análise — mapeamento 14→8 é limpo; ou (b) desagregar 2015 via descrições de ações do 532 (muitas citam municípios) |
| Validação externa | Parser BGE (2015-2022) + SICONFI DCA (total estadual por elemento) | confere níveis e tendências; protege contra erro de coleta |
| Extensão municipal (obras) | xlsx Obras + join SACC | painel município×ano para o elemento 51, com rateio temporal documentado |
| Monitorar | `api-dados-abertos` swagger | API está sendo liberada incrementalmente; se surgir endpoint de despesas com elemento+favorecido, ganha-se o municipal completo sem WAF |

**Descartar/adiar:** solver de WAF com Playwright no Ceará Transparente (alto esforço/fragilidade para dado que o 544 já entrega melhor) e o proxy de convênios (semântica errada para o objeto da tese).

---

## 4. O que NENHUM caminho resolve — fica para LAI / contato IPECE-SEPLAG

1. **Nível municipal por elemento 51/52 do universo completo da execução.** Não existe classificador municipal no SIOF (29 dropdowns e 132 relatórios verificados); as notas de empenho com favorecido/município do Ceará Transparente seguem 100% atrás do AWS WAF (bloqueio real e atual — diferente do ESTBAN, este não está desatualizado). Único caminho limpo: **pedido LAI à SEPLAG/CGE** (o dado existe no banco — o IPECE o extraiu em 2022 e a Mensagem à Assembleia 2026 calcula shares regionais de 2024/2025).
2. **2015 nas 14 regiões de planejamento, oficialmente.** A LC 154/2015 só valeu a partir de 2016; qualquer reconstrução de 2015 em 14 regiões (via 532 ou crosswalk) é estimativa do pesquisador. Se a tese exigir 2015 oficial em 14 regiões, é LAI.
3. **Alocação do resíduo não-regionalizado de equipamentos** (código 15 "Estado do Ceará"): desprezível para obras (0,4-2,3%/ano), mas até **24,8% em 2019** para equipamentos. Nenhuma fonte pública aloca esse resíduo; a tese deve reportá-lo como limitação (ou pedir abertura via LAI).
4. **Estágio "pago" para validação externa** — o BGE só publica empenhado; o pago regionalizado fica apoiado exclusivamente no SIOF (risco aceitável: mesma fonte primária dos balanços oficiais).

Nenhum desses itens é bloqueador: a tese pede região **ou** município, e a região está integralmente coberta.

---

## 5. Recomendação final

**O SCI-01 muda de status: bloqueado → DESTRAVADO.** O dado-alvo — investimento estadual executado (empenhado e pago) em obras (51) e equipamentos (52), pelas 14 regiões de planejamento, anual, 2015-2025 — existe, é público, e **já foi coletado e validado** via SIOF relatório 544 durante esta investigação (consolidado em `/tmp/siof_544_consolidado_2015_2025.csv`; ex. obras empenhado: 2015 R$1.466mi … 2025 R$2.581mi). O bloqueio original era um defeito de leitura do coletor, não da fonte. Falta apenas trabalho de engenharia pequeno e de baixo risco (módulo `siof_regiao.py`, P; parser BGE de validação, P) e duas decisões metodológicas a registrar na tese: o tratamento de 2015 (agregar a série às 8 macrorregiões antigas ou desagregar via relatório 532) e o resíduo não-regionalizado de equipamentos (mediana ~8%, pior ano 2019 com 24,8%). A LAI à SEPLAG deixa de ser pré-requisito e vira refinamento opcional (2015 em 14 regiões oficial, recorte municipal por elemento, alocação do resíduo).
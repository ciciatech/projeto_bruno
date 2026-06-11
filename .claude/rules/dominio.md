---
description: Domínio econômico-acadêmico do projeto Bruno (doutorado DESP/UFC sobre investimento → emprego no CE)
---

# Domínio · projeto Bruno

Doutorado de Bruno Cardoso (DESP/UFC, orientador Prof. Paulo Matos — CAEN/UFC). Reformulação abr/2026: o exercício empírico passou de UF × bimestre para 14 regiões SEPLAG/IPECE × meses.

## Conceitos centrais

| Termo | Definição operacional no projeto |
|-------|----------------------------------|
| **Região de planejamento** | 14 macrorregiões do CE definidas pela SEPLAG e usadas pelo SIOF (códigos `01`–`14`). Mapeamento município → região em `pipeline/data/municipios_ce_regioes.csv` (184 municípios, fonte: IPECE TD 111/2015). |
| **Investimento estadual** | Despesas executadas pelo Estado em obras e equipamentos (categoria `4` no SIOF-CE). Granularidade nativa: região × ano. Replicado mensalmente no painel. |
| **Investimento federal** | Aplicações diretas + transferências voluntárias federais (RREO + Portal da Transparência). Granularidade: estadual mensal, replicado nas 14 regiões. |
| **Investimento municipal** | Despesas em investimento (`cod_conta = "Investimentos"`) reportadas pelos 184 municípios CE no RREO Anexo 01 do SICONFI. Bimestral acumulado-no-ano → fluxo mensal. |
| **Investimento privado residual** | Definido por exclusão: `total - estadual - municipal - federal`, com `total = FBCF Brasil mensal × 2,2%` (share PIB CE/BR) — fórmula aprovada pelo Prof. Paulo em abr/2026 (ver `docs/metodologia-composicao-investimento.md`). |
| **Endógenas** | Saldo de empregos formais (CAGED, mensal municipal) e salário médio regional. |
| **Variável de impacto** | Investimento estadual em obras (SIOF-CE). |
| **Controles regionais** | Bolsa Família, BPC, transferências constitucionais STN (FPM/FUNDEB/ITR/royalties), transferências estaduais (SEFAZ-CE via SICONFI Anexo 03), crédito via ESTBAN verbete 160 (operações de crédito por município — confirmado pelo Paulo em 10/06; reconciliar com o recorte BNB-only do adapter legado). Candidatas adicionais: séries SGS da letter Matos & Araújo (T25/T31). |
| **Controle estadual** | IBCR-CE com ajuste sazonal (BACEN SGS série 25380). Replicado nas 14 regiões. |

## Schemas canônicos

### Painel modelo-pronto mensal (`dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv`)
- 2.016 linhas = 14 regiões × 144 meses (2015-01 a 2026-12).
- Chaves: `regiao_codigo` (str, 2 dígitos zero-padded), `regiao_nome`, `ano` (int), `mes` (int).
- Colunas atualmente populadas: ver `frontend/scripts/build-data.py` (mapeamento `COL_RENAME`).

### Painel bimestral regional (`dados_nordeste/processed/model_ready/painel_regional_ce_bimestral.csv`)
- 1.008 linhas = 14 regiões × 72 bimestres (`15b1`–`26b6`), 56 colunas — 20 delas `*_real` em R$ dez/25 (base de trabalho; confirmação formal pendente com o Prof. Paulo).
- Gerado por `pipeline/transform/painel_bimestral.py` (T27); regras de agregação por coluna em `pipeline/data/matriz_regras_regional.csv` (fonte de verdade, lida em runtime).
- Export xlsx no layout do Prof. Paulo em `dados_nordeste/processed/exports/`.

### Outputs por coletor
Todos seguem o padrão `(cod_ibge, regiao_codigo, regiao_nome, ano, mes, ...valores)` — usar `pipeline.regioes_ce.agregar_para_regiao` quando o coletor é municipal e o consumidor (painel) precisa regional.

## Padrões a respeitar

1. **Nunca recriar mapeamento município → região** — sempre `pipeline.regioes_ce.get_regiao_info()` ou `agregar_para_regiao`.
2. **Aliases ortográficos** já tratados em `_ALIASES_NOME` (Itapagé ↔ Itapajé, Lei CE 16.550/2018). Não duplicar em outros lugares.
3. **Ano-base monetário**: nominal por padrão nas fontes. Deflator IPCA implementado em `pipeline/transform/deflator.py` (T28): base **pinada dez/25, parametrizável** (dez/24 pronto se o Paulo preferir), validado contra a planilha dele com erro 2×10⁻¹⁵. A base dez/25 é a de trabalho — confirmação formal pendente.
4. **Período** padrão: 2015–2026 mensal. `PERIODO_INICIO_MENSAL` / `PERIODO_FIM_MENSAL` em `pipeline/config.py`.
5. **CE = UF 23**. `COD_IBGE_CE = "23"`, `UF_FOCO = "CE"`. Fonte canônica em `pipeline/config.py`.
6. **Nomenclatura de variáveis no domínio em português** (`regiao_codigo`, `salario_medio`, `invest_total`); nomes de infra em inglês (`Dockerfile`, `nginx.conf`, `vite.config.ts`).

## Decisões aprovadas pelo Prof. Paulo (abr/2026)

- Investimento total CE para o residual privado: **FBCF Brasil mensal × 2,2%** (share PIB CE/BR).
- Ano-base do residual privado: **R$ 2024/2025** (não R$ 2010 da FBCF nativa) — base **em revisão → dez/25** (planilha bimestral jun/2026; ver Decisões pendentes).
- CAGED municipal: **vale o custo** (~24h FTP MTE — rodou em 59min em 2026-04-30).
- SEFAZ-CE: **substituída pelo SICONFI Anexo 03** (cota-parte ICMS/IPVA); o adapter manual (site bloqueia bots) fica como fallback.
- Investimento municipal: **SICONFI automático** (não a planilha manual do Bruno).

## Decisões pendentes (estado 2026-06-10; bloqueiam tasks)

- **4 confirmações com o Prof. Paulo** (planilha bimestral jun/2026, ver `docs/estudo-viabilidade-painel-bimestral.md` e T18): cota-parte ICMS como proxy de "ICMS recolhido"; shares federais 15,4% regional (vs 14,5% NE de abr/2026); base monetária dez/25 substitui dez/24; investimento municipal EMPENHADO serve ou precisa recoletar PAGO (T34).
- Variáveis de controle definitivas do modelo causal (Tela 4) — especificação econométrica (T05.2). Candidatas: séries SGS da letter Matos & Araújo (T25/T31), SELIC, IBCR-CE.
- Transferência estadual = **SIOF Função 08 / Subfunções 241–244** (Mais Infância, Ceará sem Fome, Vale Gás Social) — novo recorte (T24), sujeito ao SCI-01 (sem desagregação regional <2026).
- ESTBAN verbete 160 por município (T16) — fonte confirmada pelo Paulo em 10/06, mas acesso bloqueado (BCB removeu URL pública estável).

## Fontes externas usadas

| Fonte | Endpoint / arquivo | Cadência | Coletor |
|-------|--------------------|----------|---------|
| SICONFI / Tesouro · RREO Anexo 01 (invest. mun.) | `apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo` | Bimestral | `pipeline/extract/invest_municipal_siconfi.py` |
| SICONFI / Tesouro · RREO Anexo 03 (cota-parte ICMS/IPVA) | idem | Bimestral (12-mes window) | `pipeline/extract/sefaz_ce_siconfi.py` |
| Portal da Transparência | API CSV bulk | Mensal | `bolsa_familia_ce.py` · `bpc.py` · `invest_federal.py` |
| BACEN SGS | `api.bcb.gov.br/dados/serie/bcdata.sgs` | Mensal | `pipeline/extract/bacen.py` (IBCR-CE série 25380) |
| IpeaData (FBCF) | `ipeadata.gov.br/api` | Mensal | `pipeline/extract/ipea_fbcf.py` |
| MTE / CAGED | FTP `ftp.mtps.gov.br/pdet/microdados` | Mensal | `pipeline/extract/caged_municipal.py` |
| IBGE / SIDRA tabela 6579 (estimativas pop.) | `servicodados.ibge.gov.br/api/v3/agregados/6579` | Anual | `pipeline/extract/populacao_ibge.py` |
| IBGE / API localidades | `servicodados.ibge.gov.br/api/v1/localidades` | Estática | `pipeline/regioes_ce.py` |
| SIOF-CE | PDF parsing local da SEPLAG | Anual (corrente) | `pipeline/extract/siof.py` |
| STN · Transferências constitucionais | CKAN STN | Mensal | `pipeline/extract/transferencias_municipais.py` |

**Bloqueados por terceiros:**
- ESTBAN (verbete 160, operações de crédito por município) — BCB removeu URL pública estável; adapter manual em `dados_nordeste/raw/estban/`
- SEFAZ-CE direto — site bloqueia bots; **agora substituído pelo SICONFI Anexo 03** (cota-parte ICMS/IPVA), adapter manual fica como fallback

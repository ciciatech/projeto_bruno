---
description: Domínio econômico-acadêmico do projeto Bruno (tese DESP/UFC sobre investimento → emprego no CE)
---

# Domínio · projeto Bruno

Tese de Bruno Cardoso (DESP/UFC, orientador Prof. Paulo Araújo). Reformulação abr/2026: o exercício empírico passou de UF × bimestre para 14 regiões SEPLAG/IPECE × meses.

## Conceitos centrais

| Termo | Definição operacional no projeto |
|-------|----------------------------------|
| **Região de planejamento** | 14 macrorregiões do CE definidas pela SEPLAG e usadas pelo SIOF (códigos `01`–`14`). Mapeamento município → região em `pipeline/data/municipios_ce_regioes.csv` (184 municípios, fonte: IPECE TD 111/2015). |
| **Investimento estadual** | Despesas executadas pelo Estado em obras e equipamentos (categoria `4` no SIOF-CE). Granularidade nativa: região × ano. Replicado mensalmente no painel. |
| **Investimento federal** | Aplicações diretas + transferências voluntárias federais (RREO + Portal da Transparência). Granularidade: estadual mensal, replicado nas 14 regiões. |
| **Investimento municipal** | Despesas em investimento (`cod_conta = "Investimentos"`) reportadas pelos 184 municípios CE no RREO Anexo 01 do SICONFI. Bimestral acumulado-no-ano → fluxo mensal. |
| **Investimento privado residual** | Definido por exclusão: `total - estadual - municipal - federal`. Bloqueado: Prof. Paulo precisa decidir a fonte do "total" (FBCF nacional × share PIB / JUCEC / PIA). |
| **Endógenas** | Saldo de empregos formais (CAGED, mensal municipal) e salário médio regional. |
| **Variável de impacto** | Investimento estadual em obras (SIOF-CE). |
| **Controles regionais** | Bolsa Família, BPC, transferências constitucionais STN (FPM/FUNDEB/ITR/royalties), transferências estaduais SEFAZ-CE, crédito BNB ESTBAN. |
| **Controle estadual** | IBCR-CE com ajuste sazonal (BACEN SGS série 25380). Replicado nas 14 regiões. |

## Schemas canônicos

### Painel modelo-pronto (`dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv`)
- 1.848 linhas = 14 regiões × 132 meses (2015-01 a 2025-12).
- Chaves: `regiao_codigo` (str, 2 dígitos zero-padded), `regiao_nome`, `ano` (int), `mes` (int).
- Colunas atualmente populadas: ver `frontend/scripts/build-data.py` (mapeamento `COL_RENAME`).

### Outputs por coletor
Todos seguem o padrão `(cod_ibge, regiao_codigo, regiao_nome, ano, mes, ...valores)` — usar `pipeline.regioes_ce.agregar_para_regiao` quando o coletor é municipal e o consumidor (painel) precisa regional.

## Padrões a respeitar

1. **Nunca recriar mapeamento município → região** — sempre `pipeline.regioes_ce.get_regiao_info()` ou `agregar_para_regiao`.
2. **Aliases ortográficos** já tratados em `_ALIASES_NOME` (Itapagé ↔ Itapajé, Lei CE 16.550/2018). Não duplicar em outros lugares.
3. **Ano-base monetário**: nominal por padrão. Para o residual privado, decisão é R$ 2024/2025 (Paulo). Quando deflacionar, usar IPCA cheio (próxima task).
4. **Período** padrão: 2015–2025 mensal. `PERIODO_INICIO_MENSAL` / `PERIODO_FIM_MENSAL` em `pipeline/config.py`.
5. **CE = UF 23**. `COD_IBGE_CE = "23"`, `UF_FOCO = "CE"`. Fonte canônica em `pipeline/config.py`.
6. **Nomenclatura de variáveis no domínio em português** (`regiao_codigo`, `salario_medio`, `invest_total`); nomes de infra em inglês (`Dockerfile`, `nginx.conf`, `vite.config.ts`).

## Decisões aprovadas pelo Prof. Paulo (abr/2026)

- Ano-base do residual privado: **R$ 2024/2025** (não R$ 2010 da FBCF nativa).
- CAGED municipal: **vale o custo** (~24h FTP MTE — rodou em 59min em 2026-04-30).
- SEFAZ-CE manual: **prioridade**, mesmo sendo adapter (site bloqueia bots).
- Investimento municipal: **SICONFI automático** (não a planilha manual do Bruno).

## Decisões pendentes (bloqueiam tasks)

- Fonte do "investimento total privado" para residual.
- Variáveis de controle definitivas do modelo causal (Tela 4) — especificação econométrica.

## Fontes externas usadas

| Fonte | Endpoint / arquivo | Cadência |
|-------|--------------------|----------|
| SICONFI / Tesouro | `apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo` | Bimestral |
| Portal da Transparência | API CSV bulk (transf., BF, BPC) | Mensal |
| BACEN SGS | `api.bcb.gov.br/dados/serie/bcdata.sgs` | Mensal |
| IpeaData (FBCF) | `ipeadata.gov.br/api` | Mensal |
| MTE / CAGED | FTP `ftp.mtps.gov.br/pdet/microdados` | Mensal |
| IBGE | `servicodados.ibge.gov.br/api/v1/localidades` | Estática |
| SIOF-CE | PDF parsing local | Anual |

ESTBAN BNB e SEFAZ-CE não têm fonte automatizada (BCB removeu URL pública estável; Ceará Transparente bloqueia bots) — são adapters manuais.

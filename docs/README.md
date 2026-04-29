# Pipeline de Dados - Tese DESP/UFC

## Impactos do Credito e do Emprego no Nordeste

**Autor:** Bruno Cardoso Costa
**Orientador:** Prof. Dr. Magno Prudencio de Almeida Filho
**Programa:** Doutorado Profissional em Economia do Setor Publico (DESP/UFC)
**Periodo de analise:** 2015-2025 | **Defesa prevista:** marco/2028

Pipeline Python para coleta, organizacao, preparacao e auditoria de dados publicos usados na tese de doutorado. O objetivo e sustentar a analise do efeito do credito sobre o emprego formal no Nordeste entre 2015 e 2025, com controles macroeconomicos, fiscais e de transferencias publicas.

---

## Visao Geral

O pipeline esta organizado em quatro camadas:

1. `extract`: coleta dados brutos de APIs, FTPs e portais publicos.
2. `transform`: padroniza e organiza os dados em saidas por fonte, UF e tema.
3. `preparacao_modelo`: aplica deflacionamento e harmonizacao temporal para o modelo.
4. `quality`: audita cobertura, nulos, duplicidade e consistencia minima dos dados coletados.

A pipeline opera em **dois escopos paralelos**:

- **NE legado** (UF x bimestre, 9 estados do Nordeste). Painel: `painel_tese_bimestral.csv`.
- **CE regional** (municipio -> 14 regioes de planejamento da SEPLAG/IPECE x mes), introduzido em abr/2026 a pedido do Prof. Paulo. Painel: `painel_regional_ce_mensal.csv`.

---

## Pipeline regional CE (novo)

Coletores municipais do Ceara, agregados para 14 regioes via `pipeline/regioes_ce.py` (mapeamento dos 184 municipios -> regiao baseado no IPECE TD 111/2015).

| Modulo CE | Fonte | Saida |
|-----------|-------|-------|
| `bacen` | BACEN-SGS | inclui IBCR-CE (serie 25380, ajuste sazonal) |
| `caged_municipal` | FTP MTE/PDET | emprego e salario por municipio CE/mes (pesado) |
| `bolsa_familia_ce` | Portal da Transparencia (CSV bulk) | BF / Auxilio Brasil / Novo BF municipal mensal |
| `bpc` | Portal da Transparencia (CSV bulk) | BPC municipal mensal |
| `transferencias_municipais` | CKAN STN / Tesouro Transparente | FPM, FUNDEB, ITR, royalties por municipio mensal |
| `invest_federal` | Portal da Transparencia (CSV bulk) | transferencias voluntarias federais por municipio mensal |
| `estban` | BCB Sistema Cosif (manual) | credito BNB municipal mensal — **adapter** |
| `sefaz_ce` | Ceara Transparente (manual) | ICMS/IPVA cota-parte municipal mensal — **adapter** |
| `invest_municipal` | planilha do Bruno | investimento municipal mensal — **adapter** |

### Arquivos manuais

Tres coletores sao **adapters**: nao baixam dados automaticamente, mas processam arquivos que voce coloca em diretorios. Use isso quando a fonte nao tem download programatico estavel.

- ESTBAN (BNB): salve CSVs/parquets em `dados_nordeste/raw/estban/`. Origem: Sistema Cosif (https://www.bcb.gov.br/estabilidadefinanceira/sistemacosif) ou Base dos Dados (`basedosdados.br_bcb_estban.municipio` filtrado por `id_uf = '23'`).
- SEFAZ-CE: salve XLSX/CSV em `dados_nordeste/raw/sefaz_ce/`. Origem: Ceara Transparente, exportacao manual.
- Investimento municipal: salve a planilha do Bruno em `dados_nordeste/raw/invest_municipal/`.

### Execucao

```bash
# Pipeline regional completo (coletas + painel)
python3 -m pipeline.run --full-ce

# Apenas modulos especificos
python3 -m pipeline.run --modulos-ce bolsa_familia_ce bpc transferencias_municipais

# Reconstruir o painel regional sem refazer coletas
python3 -m pipeline.run --painel-ce
```

### Painel resultante

`dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv`:
- 14 regioes x 132 meses (2015-01 a 2025-12) = 1.848 linhas.
- Endogenas: `caged_admissoes`, `caged_desligamentos`, `caged_saldo`, `salario_medio`.
- Variavel de impacto: `siof_anual_empenhado`, `siof_anual_pago` (anual replicado em meses).
- Controles regionais: `bf_*`, `bpc_*`, `transf_fed_*`, `transf_est_*`, `invest_fed_*`, `invest_mun_*`, `bnb_*`.
- Controle estadual: `ibcr_ce`.

---

## Modulos Disponiveis

### Extratores (via `--modulos`)

| Modulo | Fonte | Saida principal |
|--------|-------|-----------------|
| `bacen` | BACEN-SGS | credito, IBCR-NE, IBC-Br, SELIC, IPCA, inadimplencia (13 series) |
| `siconfi_rreo` | SICONFI/STN | receitas, despesas e resultado primario (bimestral) |
| `siconfi_rgf` | SICONFI/STN | divida e gestao fiscal (quadrimestral) |
| `siconfi_dca` | SICONFI/STN | balanco patrimonial e investimento publico (anual) |
| `transferencias` | SICONFI/RREO | transferencias por whitelist auditavel (36 contas) |
| `bolsa_familia` | SAGI/MDS + Portal da Transparencia | coleta mensal por UF quando ha API key; fallback por URLs |
| `caged_rais` | FTP MTE/PDET | CAGED antigo (2015-2019), Novo CAGED (2020+) e RAIS (2015-2022) |
| `auditoria_qualidade` | Camada interna | auditoria automatizada de 19 datasets |

### Extratores autonomos (executados via ETL)

| Extrator | Fonte | Saida |
|----------|-------|-------|
| `SiofCE` | SEPLAG/CE (WebForms) | execucao orcamentaria CE + obras por regiao/secretaria |
| `TransparenciaAL` | Portal Transparencia AL | execucao orcamentaria AL (2015-2025) |
| `TransparenciaPI` | Portal Transparencia PI | execucao orcamentaria PI (2015-2025) |

---

## Instalacao

```bash
pip install requests pandas tqdm openpyxl pyarrow py7zr
```

---

## Execucao

### Coleta rapida (apenas BACEN)

```bash
python3 -m pipeline.run --apenas-bacen
```

### Coleta principal

```bash
python3 -m pipeline.run
```

### Coleta por modulos

```bash
python3 -m pipeline.run --modulos bacen siconfi_rreo transferencias
```

### Coleta de emprego

```bash
python3 -m pipeline.run --modulos caged_rais
```

### ETL (raw -> processed)

```bash
python3 -m pipeline.transform.etl
```

### Preparacao para o modelo

```bash
python3 -m pipeline.transform.preparacao_modelo
```

### Auditoria de qualidade

```bash
# Isolada
python3 -m pipeline.quality

# Via orquestrador
python3 -m pipeline.run --modulos auditoria_qualidade
```

### Bolsa Familia com chave de API

```bash
python3 -m pipeline.run --modulos bolsa_familia --api-key SUA_CHAVE
```

### Fluxo completo

```bash
python3 -m pipeline.run --full
```

---

## Estrutura de Saida

```text
dados_nordeste/
+-- raw/                          # ~101 CSVs brutos por fonte
|   +-- bacen/nacional/           # Series BACEN-SGS
|   +-- caged/nordeste/           # CAGED antigo + novo + manifestos
|   +-- rais/nordeste/            # RAIS vinculos e setorial
|   +-- siconfi/nordeste/         # RREO, RGF, DCA
|   +-- transferencias/nordeste/  # Transferencias constitucionais
|   +-- bolsa_familia/nordeste/   # Bolsa Familia (quando ha API key)
|   +-- execucao_orcamentaria/    # CE (SIOF + obras), AL, PI
+-- processed/                    # ~134 CSVs tratados
|   +-- model_ready/              # Saidas finais para modelagem (9 arquivos)
|   +-- caged/nordeste/           # Bimestral unificado + por UF
|   +-- siconfi_rreo/<uf>/        # Resultado primario por estado
|   +-- siconfi_rgf/<uf>/         # Divida por estado
|   +-- execucao_orcamentaria/ce/ # SIOF consolidado + obras por secretaria/regiao
+-- quality/                      # Relatorios de qualidade (JSON, MD, CSV)
+-- logs/                         # Logs de execucao
+-- metadata_coleta.json          # Metadados da ultima execucao
```

### Painel final (model_ready)

| Arquivo | Descricao |
|---------|-----------|
| `painel_tese_bimestral.csv` | Base principal: 594 linhas (9 UFs x 66 bimestres), 37 colunas |
| `bacen_bimestral.csv` | Series deflacionadas de credito |
| `caged_bimestral.csv` | Emprego por UF/bimestre |
| `resultado_primario_bimestral.csv` | Resultado primario por UF |
| `dcl_bimestral.csv` | Divida consolidada por UF |
| `investimento_publico_bimestral.csv` | Investimento publico por UF |
| `transferencias_bimestrais.csv` | Transferencias federais por UF |
| `rais_bimestral.csv` | Estoque de emprego formal |
| `matriz_regras_modelo.csv` | Regras de agregacao e deflacionamento |

---

## Series BACEN Coletadas

| Codigo | Descricao |
|--------|-----------|
| `25389` | IBCR Nordeste com ajuste sazonal |
| `14084` | Saldo de credito PF - Nordeste |
| `14089` | Saldo de credito PJ - Nordeste |
| `14079` | Saldo de credito total - Nordeste |
| `4189` | SELIC mensal |
| `433` | IPCA mensal |
| `22109` | PIB trimestral em indice |
| `21084` | Inadimplencia PF |
| `21085` | Inadimplencia PJ |
| `20539` | Credito PF Brasil |
| `20541` | Credito PJ Brasil |
| `24364` | IBC-Br |

---

## Observacoes Importantes

- O `SICONFI` pode demorar bastante por conta de volume e rate limit.
- A coleta de `CAGED` e `RAIS` usa a rota FTP do MTE/PDET.
- `RAIS` usa filtro explicito de `vinculo_ativo` na agregacao de estoque.
- O painel final (`painel_tese_bimestral.csv`) tem 594 linhas x 37 colunas, com 9.69% de missing (concentrado em variaveis com lacunas conhecidas).
- `Bolsa Familia` usa SAGI/MDS como fonte principal; Portal da Transparencia como fallback (requer API key).
- `BPC` continua pendente como controle assistencial separado.
- O SIOF-CE nao possui API REST — usa WebForms ASP.NET com ViewState (scraping).
- Os dados sao salvos em CSV. O script e idempotente: uma nova execucao sobrescreve os arquivos de saida.

---

## Limitacoes Conhecidas

- O painel final depende da qualidade upstream das fontes mais frageis (RAIS).
- RAIS tem gaps em 2019 e 2020 (downloads corrompidos) e `remuneracao_media` 100% nula.
- CAGED Antigo tem 13 meses ausentes (arquivos .7z corrompidos no FTP).
- Execucao orcamentaria cobre apenas 3 de 9 estados (CE, AL, PI).
- A harmonizacao bimestral cobre o painel principal, porem parte da semantica temporal exige validacao econometrica fina.

---

## Changelog

### [2026-03-06] Qualidade, Model-Ready e SIOF Obras

- Adicionado `pipeline/quality.py` — auditoria automatizada de 19 datasets.
- Camada `model_ready` com painel bimestral final (594 linhas, 37 colunas).
- SIOF-CE expandido: coleta de obras por regiao (14 regioes CE) e por secretaria.
  - Filtros por elemento de despesa (51-Obras, 52-Equipamentos) e grupo (44-Investimentos).
  - ETL gera `siof_obras_secretaria.csv` e `siof_obras_regiao.csv`.
- Transparencia PI adicionada como extrator (2015-2025).
- Corrigida agregacao de `RAIS` para usar `vinculo_ativo`.
- Coleta de `transferencias` com whitelist auditavel de 36 `cod_conta`.
- `Bolsa Familia` com fallback SAGI/MDS + Portal de Dados Abertos.
- `pipeline/run.py` suporta modos `--etl`, `--preparar-modelo`, `--auditar` e `--full`.

### [2025-02-24] Redirecionamento da Tese

A tese deixa de focar exclusivamente na relacao credito -> IBCR-NE e incorpora **emprego como variavel dependente principal**, respondendo:

> "Se dermos R$ 1 bilhao em credito, quantos mil empregos sao gerados?"

- **Variavel dependente**: Volume de empregos (formais, com proxy para informais)
- **Variaveis explicativas**: Credito PF (familias) e Credito PJ (empresas)
- **Controles**: IBCR-NE, transferencias federais (BPC, Bolsa Familia), transferencias estaduais (SIOF), investimento publico
- **Produto Tecnologico**: separado estrategicamente da tese academica (Prof. Paulo Matos)

---

## Documentacao Complementar

| Documento | Conteudo |
|-----------|----------|
| [dicionario_dados.md](dicionario_dados.md) | Dicionario completo de todas as variaveis e arquivos |
| [relatorio_qualidade.md](relatorio_qualidade.md) | Relatorio consolidado de qualidade dos dados |
| [analise_projeto.md](analise_projeto.md) | Analise do projeto, diagnostico e recomendacoes |
| [analise_tecnica_rag.md](analise_tecnica_rag.md) | Analise tecnica do sistema RAG hibrido (BNB) |
| [pdf/](pdf/) | Documentos do seminario de tese |

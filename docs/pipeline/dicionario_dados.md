# Dicionário de Dados — Pipeline Tese DESP/UFC

**Projeto:** Impactos do Crédito e do Emprego no Nordeste
**Última atualização:** 06/03/2026
**Cobertura territorial:** 9 estados do Nordeste (AL, BA, CE, MA, PB, PE, PI, RN, SE)
**Período alvo:** 2015–2025

---

## 1. DADOS BRUTOS (`dados_nordeste/raw/`)

### 1.1 BACEN — Séries Temporais SGS

**Fonte:** API SGS do Banco Central do Brasil
**Coleta:** `pipeline/extract/bacen.py`

| Arquivo | Registros | Período | Frequência |
|---------|-----------|---------|------------|
| `bacen/nacional/bacen_sgs_series.csv` | 1.496 | 2015–2025 | Mensal |
| `bacen/nacional/bacen_sgs_wide.csv` | 132 | 2015–2025 | Mensal |

**Séries coletadas (12):**

| Código SGS | Nome no arquivo | Descrição | Unidade |
|-----------|----------------|-----------|---------|
| 25389 | `IBCR_NE_ajuste_sazonal` | Índice de Atividade Econômica Regional — NE | Índice (2002=100) |
| 14084 | `credito_PF_nordeste` | Saldo de crédito Pessoa Física — NE | R$ milhões |
| 14089 | `credito_PJ_nordeste` | Saldo de crédito Pessoa Jurídica — NE | R$ milhões |
| 14079 | `credito_total_nordeste` | Saldo de crédito total — NE | R$ milhões |
| 4189 | `selic_mensal` | SELIC acumulada no mês (meta COPOM) | % a.m. |
| 433 | `ipca_mensal` | IPCA — variação mensal | % |
| 22109 | `pib_trimestral_indice` | PIB trimestral — índice de volume | Índice |
| 21084 | `inadimplencia_PF` | Inadimplência PF — operações de crédito | % |
| 21085 | `inadimplencia_PJ` | Inadimplência PJ — operações de crédito | % |
| 20539 | `credito_PF_brasil` | Saldo de crédito PF — Brasil | R$ milhões |
| 20541 | `credito_PJ_brasil` | Saldo de crédito PJ — Brasil | R$ milhões |
| 24364 | `ibc_br` | IBC-Br — proxy mensal do PIB nacional | Índice (2002=100) |

**Colunas `bacen_sgs_series.csv`:**
- `data` — data de referência (datetime)
- `valor` — valor da série (float)
- `serie_codigo` — código SGS (int)
- `serie_nome` — nome da série (string)

**Colunas `bacen_sgs_wide.csv`:** `data` + uma coluna por série (pivot)

---

### 1.2 CAGED Antigo — Emprego Formal 2015–2019

**Fonte:** FTP MTE/PDET — `ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/`
**Coleta:** `pipeline/extract/caged_rais.py` → `CagedRais.coletar_caged_antigo_nordeste()`
**Layout:** pré-eSocial (campo `admitidos_desligados`: 1=admissão, 2=desligamento)

| Arquivo | Registros | Período | Frequência |
|---------|-----------|---------|------------|
| `caged/nordeste/caged_antigo_saldo_mensal.csv` | ~540 | 2015–2019 | Mensal × UF |
| `caged/nordeste/caged_antigo_por_perfil.csv` | ~2.700 | 2015–2019 | Anual × UF × sexo × instrução |

**Colunas `caged_antigo_saldo_mensal.csv`:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ano` | int | Ano de referência |
| `mes` | int | Mês (1–12) |
| `sigla_uf` | str | UF do Nordeste (AL, BA, CE, ...) |
| `admissoes` | int | Total de admissões no mês |
| `desligamentos` | int | Total de desligamentos no mês |
| `saldo` | int | Saldo líquido (admissões − desligamentos) |
| `salario_medio` | float | Salário médio das movimentações (R$) |
| `total_movimentacoes` | int | Total de registros no mês |

---

### 1.3 CAGED Novo — Emprego Formal 2020–2025

**Fonte:** FTP MTE/PDET — `ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/`
**Coleta:** `pipeline/extract/caged_rais.py` → `CagedRais.coletar_caged_nordeste()`
**Layout:** eSocial (campo `saldo_movimentacao`: +1=admissão, −1=desligamento)

| Arquivo | Registros | Período | Frequência |
|---------|-----------|---------|------------|
| `caged/nordeste/caged_saldo_mensal.csv` | 648 | 2020–2025 | Mensal × UF |
| `caged/nordeste/caged_por_setor.csv` | 4.263 | 2020–2025 | Anual × UF × divisão CNAE |
| `caged/nordeste/caged_por_perfil.csv` | 1.317 | 2020–2025 | Anual × UF × sexo × instrução |

**Colunas:** mesma estrutura do CAGED Antigo (esquema harmonizado na coleta).

---

### 1.4 RAIS — Estoque de Vínculos Empregatícios

**Fonte:** FTP MTE/PDET — `ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/`
**Coleta:** `pipeline/extract/caged_rais.py` → `CagedRais.coletar_rais_nordeste()`
**Processamento:** leitura em chunks de 500k linhas (arquivos de 11–13M registros)

| Arquivo | Registros | Período | Frequência |
|---------|-----------|---------|------------|
| `rais/nordeste/rais_vinculos.csv` | 47 | 2015–2022 | Anual × UF |
| `rais/nordeste/rais_por_setor.csv` | 3.732 | 2015–2022 | Anual × UF × divisão CNAE |

**Cobertura por ano:**

| Ano | Formato FTP | Status | UFs coletadas |
|-----|------------|--------|---------------|
| 2015 | `{UF}2015.7z` (por UF) | Parcial | AL, MA, RN, SE (BA, CE, PB, PE, PI corrompidos) |
| 2016 | `{UF}2016.7z` (por UF) | Parcial | 8 de 9 (PE corrompido) |
| 2017 | `{UF}2017.7z` (por UF) | Parcial | 8 de 9 (BA corrompido) |
| 2018 | `RAIS_VINC_PUB_NORDESTE.7z` | Completo | 9 de 9 (11,5M registros) |
| 2019 | `RAIS_VINC_PUB_NORDESTE.7z` | Falhou | Download corrompido |
| 2020 | `RAIS_VINC_PUB_NORDESTE.7z` | Falhou | Download corrompido |
| 2021 | `RAIS_VINC_PUB_NORDESTE.7z` | Completo | 9 de 9 (12,1M registros) |
| 2022 | `RAIS_VINC_PUB_NORDESTE.7z` | Completo | 9 de 9 (13,5M registros) |
| 2023 | `RAIS_VINC_PUB_NORDESTE.7z` | Falhou | Formato `.COMT` (não CSV) |

**Colunas `rais_vinculos.csv`:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ano` | int | Ano de referência |
| `sigla_uf` | str | UF do Nordeste |
| `vinculos_ativos` | int | Estoque de vínculos ativos em 31/12 |
| `remuneracao_media` | float | Remuneração média nominal (R$) |

**Colunas `rais_por_setor.csv`:** idem + `divisao_cnae` (2 dígitos CNAE 2.0)

---

### 1.5 SICONFI — Dados Fiscais Estaduais (STN)

**Fonte:** API SICONFI — `https://apidatalake.tesouro.gov.br/ords/siconfi/tt/`
**Coleta:** `pipeline/extract/siconfi.py`

| Arquivo | Registros | Período | Frequência |
|---------|-----------|---------|------------|
| `siconfi/nordeste/siconfi_rreo_nordeste.csv` | 1.987.525 | 2015–2025 | Bimestral × UF |
| `siconfi/nordeste/siconfi_rgf_nordeste.csv` | 95.759 | 2015–2025 | Quadrimestral × UF |
| `siconfi/nordeste/siconfi_dca_nordeste.csv` | 232.905 | 2015–2024 | Anual × UF |

**Colunas comuns:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `exercicio` | int | Ano fiscal |
| `periodo` | int | Bimestre (1–6 RREO) ou quadrimestre (1–3 RGF) |
| `uf` | str | Sigla da UF |
| `anexo` | str | Anexo do relatório (RREO-Anexo 01, RGF-Anexo 02, etc.) |
| `cod_conta` | str | Código da conta contábil |
| `conta` | str | Nome da conta |
| `coluna` | str | Tipo de valor (Até o Bimestre, Previsão Atualizada, etc.) |
| `valor` | float | Valor em R$ |
| `populacao` | int | População do ente |

---

### 1.6 Transferências Constitucionais

**Fonte:** API SICONFI (RREO Anexos 01 e 06) — filtro por contas de transferência
**Coleta:** `pipeline/extract/transferencias.py`

| Arquivo | Registros | Período | Frequência |
|---------|-----------|---------|------------|
| `transferencias/nordeste/transferencias_constitucionais_nordeste.csv` | 42.205 | 2015–2025 | Bimestral × UF |

**Termos filtrados:** FPE, FPM, FUNDEB, CIDE, royalties, compensações financeiras.

---

### 1.7 Bolsa Família

**Fonte:** Portal da Transparência com agregação por UF; fallback por URLs de download
**Coleta:** `pipeline/extract/bolsa_familia.py`

| Arquivo | Registros | Período | Cobertura |
|---------|-----------|---------|-----------|
| `bolsa_familia/nordeste/bolsa_familia_uf_mensal.csv` | variável | 2015–2025 | 9 UFs, quando há API key |
| `bolsa_familia/nordeste/bolsa_familia_portal_transparencia.csv` | variável | 2015–2025 | Registros municipais brutos, quando há API key |
| `bolsa_familia/nacional/bolsa_familia_urls_download.csv` | 132 | 2015–2025 | URLs para download |

**Limitação:** sem API key, a pipeline faz fallback para URLs; `BPC` continua ausente.

---

### 1.8 Execução Orçamentária Estadual

**Fonte:** Portais de transparência estaduais
**Coleta:** `pipeline/extract/siof.py`, `transparencia_al.py`, `transparencia_pi.py`

| Arquivo | Registros | Período | Estado |
|---------|-----------|---------|--------|
| `execucao_orcamentaria/ce/siof_consolidado.csv` | 480 | 2015–2026 | Ceará |
| `execucao_orcamentaria/al/transparencia_al_consolidado.csv` | 2.065 | 2015–2025 | Alagoas |

**Colunas CE (SIOF):** `Código`, `Descrição`, `Lei`, `Lei + Cred.`, `Empenhado`, `Pago`, `% Emp.`, `% Pago`, `ano`, `mes`

**Colunas AL:** `dotacao_inicial`, `dotacao_atualizada`, `empenhado`, `liquidado`, `pago`, `unidade_gestora`, `cod_ug`, `funcao`, `ano`

---

## 2. DADOS PROCESSADOS (`dados_nordeste/processed/`)

### 2.1 BACEN — Séries Tratadas

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `bacen/nacional/bacen.csv` | 132 | Séries mensais originais (wide) |
| `bacen/nacional/bacen_deflacionado.csv` | 132 | Séries com crédito deflacionado pelo IPCA (base dez/2025=100) |
| `bacen/nacional/bacen_bimestral.csv` | 66 | Séries harmonizadas para periodicidade bimestral |

**Colunas adicionais em `bacen_deflacionado.csv`:**
- `credito_PF_brasil_real` — Crédito PF Brasil em R$ reais (dez/2025)
- `credito_PF_nordeste_real` — Crédito PF NE em R$ reais
- `credito_PJ_brasil_real` — Crédito PJ Brasil em R$ reais
- `credito_PJ_nordeste_real` — Crédito PJ NE em R$ reais
- `credito_total_nordeste_real` — Crédito total NE em R$ reais

**Colunas `bacen_bimestral.csv`:**
- `ano_bim` — ano de referência
- `bimestre` — bimestre (1–6)
- Taxas (média bimestral): `selic_mensal`, `ipca_mensal`, `inadimplencia_PF`, `inadimplencia_PJ`
- Estoques (último valor do bimestre): `credito_*`, `ibc_br`, `IBCR_NE_ajuste_sazonal`, `pib_trimestral_indice`

---

### 2.2 CAGED — Emprego Formal Processado

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `caged/nordeste/caged_antigo_saldo_mensal.csv` | ~540 | CAGED Antigo processado (2015–2019) |
| `caged/nordeste/caged_saldo_mensal.csv` | 648 | CAGED Novo processado (2020–2025) |
| `caged/nordeste/caged_bimestral.csv` | 594 | **Série unificada bimestral (2015–2025)** |
| `caged/nordeste/caged_por_setor.csv` | 4.263 | Saldo por divisão CNAE (2020–2025) |
| `caged/nordeste/caged_por_perfil.csv` | 1.317 | Saldo por sexo × escolaridade (2020–2025) |
| `caged/<uf>/caged_saldo_mensal.csv` | 72 cada | Saldo mensal por estado individual |

**Colunas `caged_bimestral.csv`:**

| Coluna | Tipo | Agregação | Descrição |
|--------|------|-----------|-----------|
| `ano_bim` | int | — | Ano |
| `bimestre` | int | — | Bimestre (1–6) |
| `sigla_uf` | str | grupo | UF do Nordeste |
| `admissoes` | int | soma | Total de admissões no bimestre |
| `desligamentos` | int | soma | Total de desligamentos no bimestre |
| `saldo` | int | soma | Saldo líquido do bimestre |
| `total_movimentacoes` | int | soma | Total de movimentações |
| `salario_medio` | float | média | Salário médio no bimestre (R$) |

---

### 2.3 RAIS — Estoque de Vínculos Processado

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `rais/nordeste/rais_vinculos.csv` | 47 | Vínculos ativos em 31/12, por UF × ano |
| `rais/nordeste/rais_por_setor.csv` | 3.732 | Vínculos por UF × ano × divisão CNAE |

---

### 2.4 SICONFI — Variáveis do Modelo Wavelet

#### Resultado Primário (RREO Anexo 06)

| Arquivo | Registros | Frequência |
|---------|-----------|------------|
| `siconfi_rreo/<uf>/rreo_resultado_primario.csv` | 36 por UF | Bimestral |

**Contas extraídas:** `ResultadoPrimarioComRPPSAcimaDaLinha`, `ResultadoPrimarioSemRPPSAcimaDaLinha`, `ReceitaPrimariaTotal`, `DespesaPrimariaTotal`

#### Dívida Consolidada Líquida (RGF Anexo 02)

| Arquivo | Registros | Frequência |
|---------|-----------|------------|
| `siconfi_rgf/<uf>/rgf_divida.csv` | 297 por UF | Quadrimestral |

**Contas extraídas:** `DividaConsolidada`, `DividaConsolidadaLiquida`, `DividaContratual`

#### Investimento Público (DCA Anexo I-D)

| Arquivo | Registros | Frequência |
|---------|-----------|------------|
| `siconfi_dca/<uf>/dca_investimento.csv` | 20 por UF | Anual |

**Contas extraídas:** `DO4.4.00.00.00.00` (Investimentos — Despesas de Capital), `DO4.0.00.00.00.00` (Total Despesas), `DO4.3.00.00.00.00` (Despesas Correntes). Filtro: coluna "Despesas Liquidadas".

#### Resumos gerais

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `siconfi_rreo/<uf>/rreo_resumo.csv` | ~330 por UF | Receitas e despesas resumidas (Anexo 01) |
| `siconfi_rgf/<uf>/rgf_resumo.csv` | ~1.700 por UF | Despesa com pessoal + dívida (Anexos 01 e 02) |
| `siconfi_dca/<uf>/dca_resumo.csv` | 70 por UF | Balanço patrimonial (Anexo I-AB) |

---

### 2.5 Transferências Constitucionais Processadas

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `transferencias/<uf>/transferencias.csv` | ~600–820 por UF | FPE, FUNDEB, CIDE, royalties (filtro "Até o Bimestre") |

---

### 2.6 Execução Orçamentária Processada

| Arquivo | Registros | Descrição |
|---------|-----------|-----------|
| `execucao_orcamentaria/al/transparencia_al.csv` | 2.065 | Alagoas 2015–2025 |
| `execucao_orcamentaria/ce/siof_ce.csv` | 480 | Ceará 2015–2026 |

---

### 2.7 Camada de Qualidade dos Dados

A camada `quality` formaliza uma etapa metodológica importante do projeto: em dados públicos, a coleta precisa funcionar também como auditoria da confiabilidade da base.

| Arquivo | Formato | Descrição |
|---------|---------|-----------|
| `quality/quality_report.json` | JSON | Relatório completo por fonte, com métricas, alertas e status |
| `quality/quality_summary.csv` | CSV | Resumo tabular das auditorias para inspeção rápida |
| `quality/quality_report.md` | Markdown | Versão legível para documentação metodológica e uso na tese |

#### Fontes auditadas atualmente

- `bacen/nacional/bacen_sgs_wide.csv`
- `caged/nordeste/caged_antigo_saldo_mensal.csv`
- `caged/nordeste/caged_saldo_mensal.csv`
- `rais/nordeste/rais_vinculos.csv`
- `siconfi/nordeste/siconfi_rreo_nordeste.csv`
- `siconfi/nordeste/siconfi_rgf_nordeste.csv`
- `siconfi/nordeste/siconfi_dca_nordeste.csv`
- `transferencias/nordeste/transferencias_constitucionais_nordeste.csv`
- `bolsa_familia/nordeste/bolsa_familia_uf_mensal.csv`
- `bolsa_familia/nordeste/bolsa_familia_portal_transparencia.csv`
- `processed/bacen/nacional/bacen_bimestral.csv`
- `processed/caged/nordeste/caged_bimestral.csv`
- `processed/model_ready/painel_tese_bimestral.csv`
- `processed/execucao_orcamentaria/al/transparencia_al.csv`
- `processed/execucao_orcamentaria/ce/siof_ce.csv`
- `processed/execucao_orcamentaria/pi/transparencia_pi.csv`

#### Checagens automatizadas

| Critério | Descrição | Utilidade analítica |
|----------|-----------|---------------------|
| Existência do arquivo | Verifica se a saída esperada foi gerada | Detecta falhas de coleta ou ETL |
| Cobertura temporal | Identifica anos ausentes e intervalo observado | Mede aderência ao período 2015–2025 |
| Cobertura territorial | Conta UFs presentes por dataset | Detecta cobertura parcial do Nordeste |
| Continuidade | Conta lacunas em combinações período × UF | Detecta descontinuidade na série |
| Nulos totais | Percentual global de células vazias | Sinaliza degradação da qualidade geral |
| Nulos em colunas críticas | Foco nas variáveis essenciais do dataset | Prioriza problemas com impacto no modelo |
| Duplicidade por chave | Verifica repetição em chaves esperadas | Evita contagem dupla e distorções |

#### Achados relevantes da primeira auditoria

| Fonte | Status | Achado |
|-------|--------|--------|
| `rais_vinculos.csv` | Alerta | anos `2019` e `2020` ausentes; `remuneracao_media` com nulos relevantes |
| `siconfi_dca_nordeste.csv` | Alerta | `2025` ausente, coerente com a defasagem natural da publicação |

#### Interpretação metodológica

A camada de qualidade deve ser tratada como parte da documentação científica do projeto, porque:

- valida a adequação das fontes antes da modelagem;
- ajuda a justificar exclusões, interpolação e uso de proxies;
- documenta limitações empíricas dos dados públicos;
- melhora a rastreabilidade entre coleta, transformação e inferência.

---

## 3. MAPEAMENTO PARA O MODELO WAVELET

| Papel | Variável | Arquivo processado | Frequência | Status |
|-------|----------|--------------------|------------|--------|
| **Dependente** | Emprego formal (fluxo) | `caged/nordeste/caged_bimestral.csv` | Bimestral | **Completo 2015–2025** |
| **Dependente** | Emprego formal (estoque) | `rais/nordeste/rais_vinculos.csv` | Anual | Parcial (6 de 8 anos) |
| **Explicativa** | Crédito PF NE (real) | `bacen/nacional/bacen_bimestral.csv` | Bimestral | Completo |
| **Explicativa** | Crédito PJ NE (real) | `bacen/nacional/bacen_bimestral.csv` | Bimestral | Completo |
| **Controle** | IBCR-NE | `bacen/nacional/bacen_bimestral.csv` | Bimestral | Completo |
| **Controle** | IBC-Br | `bacen/nacional/bacen_bimestral.csv` | Bimestral | Completo |
| **Controle** | SELIC | `bacen/nacional/bacen_bimestral.csv` | Bimestral | Completo |
| **Controle** | IPCA | `bacen/nacional/bacen_bimestral.csv` | Bimestral | Completo |
| **Controle** | Resultado Primário | `siconfi_rreo/<uf>/rreo_resultado_primario.csv` | Bimestral | Completo (9 UFs) |
| **Controle** | DCL | `model_ready/dcl_bimestral.csv` | Bimestral | Completo |
| **Controle** | Investimento público | `model_ready/investimento_publico_bimestral.csv` | Bimestral | Completo |
| **Controle** | Transferências (FPE, FUNDEB) | `transferencias/<uf>/transferencias.csv` | Bimestral | Completo |
| **Controle** | Bolsa Família | `bolsa_familia/nordeste/bolsa_familia_uf_mensal.csv` | Mensal | Condicional à API key |
| **Painel final** | Base model-ready da tese | `model_ready/painel_tese_bimestral.csv` | Bimestral | Completo |

---

## 4. PIPELINE DE EXECUÇÃO

```
# Etapa 1 — Coleta (APIs + FTP)
python3 -m pipeline.run --modulos bacen siconfi_rreo siconfi_rgf siconfi_dca transferencias caged_rais

# Etapa 2 — ETL (raw → processed)
python3 -m pipeline.transform.etl

# Etapa 3 — Preparação para modelo (deflação + harmonização bimestral)
python3 -m pipeline.transform.preparacao_modelo

# Etapa 4 — Auditoria de qualidade
python3 -m pipeline.quality

# Etapa opcional — fluxo completo
python3 -m pipeline.run --full
```

---

## 5. PENDÊNCIAS

| Item | Descrição | Impacto |
|------|-----------|---------|
| RAIS 2019, 2020 | Downloads corrompidos — retry necessário | Sem estoque de emprego para 2 anos |
| RAIS 2015 parcial | 5 UFs corrompidas (BA, CE, PB, PE, PI) | Estoque parcial para 2015 |
| RAIS 2023 | Formato `.COMT` (eSocial) — não CSV | Sem estoque para 2023 |
| Bolsa Família | Depende de API key para coleta persistida | Controle assistencial condicional |
| BPC | Não coletado | Controle ausente |
| Exportação/Importação NE | Não coletado (MDIC/IPEADATA) | Controle ausente |
| RAIS | Persistem gaps históricos em anos críticos | Fragilidade da variável de estoque de emprego |

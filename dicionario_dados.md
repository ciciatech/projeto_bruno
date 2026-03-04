# Dicionário de Dados — Pipeline Tese DESP/UFC

**Projeto:** Impactos do Crédito no Crescimento Econômico do Nordeste
**Última atualização:** 04/03/2026
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
| `siconfi/nordeste/siconfi_dca_nordeste.csv` | 232.905 | 2015–2025 | Anual × UF |

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

**Fonte:** Dados coletados via Portal da Transparência (capitais NE)
**Coleta:** `pipeline/extract/bolsa_familia.py`

| Arquivo | Registros | Período | Cobertura |
|---------|-----------|---------|-----------|
| `bolsa_familia/nordeste/bolsa_familia_capitais_ne.csv` | 225 | 2024–2026 | Apenas capitais |
| `bolsa_familia/nacional/bolsa_familia_urls_download.csv` | 132 | 2015–2025 | URLs para download |

**Limitação:** cobertura insuficiente para o modelo (apenas capitais, 2024+).

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
| **Controle** | DCL | `siconfi_rgf/<uf>/rgf_divida.csv` | Quadrimestral | Completo (interpolação necessária) |
| **Controle** | Investimento público | `siconfi_dca/<uf>/dca_investimento.csv` | Anual | Completo (interpolação necessária) |
| **Controle** | Transferências (FPE, FUNDEB) | `transferencias/<uf>/transferencias.csv` | Bimestral | Completo |
| **Controle** | Bolsa Família | `bolsa_familia/<uf>/bolsa_familia.csv` | Mensal | Incompleto (só capitais, 2024+) |

---

## 4. PIPELINE DE EXECUÇÃO

```
# Etapa 1 — Coleta (APIs + FTP)
python -m pipeline.run --modulos bacen siconfi_rreo siconfi_rgf siconfi_dca transferencias caged_rais

# Etapa 2 — ETL (raw → processed)
python -m pipeline.transform.etl

# Etapa 3 — Preparação para modelo (deflação + harmonização bimestral)
python -m pipeline.transform.preparacao_modelo
```

---

## 5. PENDÊNCIAS

| Item | Descrição | Impacto |
|------|-----------|---------|
| RAIS 2019, 2020 | Downloads corrompidos — retry necessário | Sem estoque de emprego para 2 anos |
| RAIS 2015 parcial | 5 UFs corrompidas (BA, CE, PB, PE, PI) | Estoque parcial para 2015 |
| RAIS 2023 | Formato `.COMT` (eSocial) — não CSV | Sem estoque para 2023 |
| Bolsa Família | Apenas capitais 2024–2026 | Controle insuficiente |
| BPC | Não coletado | Controle ausente |
| Exportação/Importação NE | Não coletado (MDIC/IPEADATA) | Controle ausente |
| DCL → bimestral | Interpolação quadrimestral→bimestral pendente | Necessário para modelo |
| Investimento → bimestral | Interpolação anual→bimestral pendente | Necessário para modelo |

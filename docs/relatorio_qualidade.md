# Relatorio Consolidado de Qualidade dos Dados

**Projeto:** Impactos do Credito e do Emprego no Nordeste
**Cobertura territorial:** 9 UFs do Nordeste (AL, BA, CE, MA, PB, PE, PI, RN, SE)
**Periodo alvo:** 2015-2025
**Ultima atualizacao:** 06/03/2026

---

## 1. BACEN (Series Macroeconomicas e Credito)

**Status:** OK

| Metrica | Valor |
|---------|-------|
| Volume | 132 registros, 13 colunas |
| Granularidade | Mensal (nivel Data) |
| Periodo | jan/2015 - dez/2025 (continuo, sem lacunas) |
| Nulos | `pib_trimestral_indice`: 88 nulos (66.7%) - esperado (dado trimestral) |
| Outliers | IBCR-NE (3 reg), IBC-Br (2), inadimplencia PF (2), IPCA (2) |

Zero nulos em: IBCR-NE, credito PF/PJ (NE e BR), Selic, IPCA, IBC-Br, inadimplencia. Todas as 12 series previstas na tese estao presentes.

### BACEN Deflacionado

Mesma estrutura (132 registros, 18 colunas) com series deflacionadas pelo IPCA (base dez/2025=100). Mesmos alertas de nulos e outliers.

---

## 2. CAGED — Emprego Formal

### 2.1 CAGED Novo (2020-2025) — 100% COMPLETO

| Metrica | Valor |
|---------|-------|
| Volume | 648 registros, 9 colunas |
| Granularidade | Mensal x UF |
| Nulos | Zero |
| Duplicatas | Zero |
| Integridade | `admissoes - desligamentos == saldo` em 648/648 registros |

Outliers identificados (IQR): admissoes (6 reg), desligamentos (9), saldo (60), salario_medio (8), total_movimentacoes (8).

### 2.2 CAGED Antigo (2015-2019) — 78.3% COMPLETO

| Metrica | Valor |
|---------|-------|
| Volume | 423 registros (esperados: 540) |
| Granularidade | Mensal x UF |
| Nulos | Zero |
| Integridade | `admissoes - desligamentos == saldo` em 423/423 registros |

**13 meses ausentes** (todas as UFs, falha sistemica no FTP):

| Ano | Meses faltantes |
|-----|-----------------|
| 2015 | jan, mar, jul, nov |
| 2016 | mar, mai |
| 2017 | abr, jun, dez |
| 2018 | nenhum (completo) |
| 2019 | jan, mar, ago, set |

Outlier notavel: `saldo` com 47 registros (11.1%) fora do IQR.

### 2.3 Serie Bimestral Unificada — 594/594 registros

Todos os bimestres presentes, mas **13 bimestres (de 9 UFs = 117 registros, 20% do total) contem dados de apenas 1 mes**, subestimando movimentacoes em ~60%.

| Bimestres parciais | Qtd. |
|--------------------|------|
| 2015-B1, B2, B4, B6 | 4 |
| 2016-B2, B3 | 2 |
| 2017-B2, B3, B6 | 3 |
| 2019-B1, B2, B4, B5 | 4 |

### 2.4 Batimentos cruzados — APROVADOS

| Teste | Resultado |
|-------|-----------|
| Saldo anual (mensal) == Saldo anual (perfil) - Antigo | 45/45 OK |
| Saldo anual (mensal) == Saldo anual (perfil) - Novo | 54/54 OK |
| Saldo anual (mensal) == Saldo anual (setor) - Novo | 54/54 OK |
| Bimestral == soma dos meses | 594/594 OK |

### 2.5 Transicao Antigo -> Novo (dez/2019 -> jan/2020)

Sem sobreposicao temporal. Salto de +9% a +42% nas admissoes e esperado (eSocial captura mais vinculos + dezembro e mes de baixa).

---

## 3. Bug Critico Corrigido: `salario_medio`

### Problema

O campo `salario_medio` continha o mesmo valor de `total_movimentacoes` em 100% dos registros dos arquivos de saldo mensal (antigo e novo).

### Causa raiz

Bug de encoding (mojibake) em `pipeline/extract/caged_rais.py`:
1. `_read_7z()` lia CSVs com `encoding="latin-1"`, mas CAGED Novo e UTF-8
2. Nomes de colunas sofriam corrupcao: `salario` -> `salÃ¡rio`
3. `_normalize_columns()` nao encontrava a coluna de salario
4. Fallback no `.agg()` retornava `count` ao inves de `mean`

### Correcoes aplicadas

| Correcao | Descricao |
|----------|-----------|
| `_fix_mojibake()` | Reverte corrupcao de encoding (`latin-1` -> `UTF-8`) |
| Mapeamento por substring | `"salario" in lc` ao inves de correspondencia exata |
| Pre-deteccao de colunas | `valorsalariofixo` tem prioridade sobre `salario` generico |
| Virgula decimal | `str.replace(",", ".")` antes de `pd.to_numeric()` no CAGED Novo |
| Harmonizacao `sexo` | `df["sexo"].replace({3: 2})` para padronizar feminino |
| Normalizacao `grau_instrucao` | Conversao para numerico com `pd.to_numeric()` |

### Resultado

| Metrica | Antes | Depois |
|---------|-------|--------|
| salario_medio = total_mov (bug) | 100% | 0% |
| Registros com salario (Antigo) | 0/423 | 423/423 |
| Registros com salario (Novo) | 18/648 | 648/648 |
| Mediana salario Antigo | N/A | R$ 1.207 |
| Mediana salario Novo | N/A | R$ 1.501 |

**Status:** Correcao aplicada. Requer re-execucao da pipeline.

---

## 4. RAIS — Estoque de Vinculos

**Status:** FRACO (problema mais grave do dataset)

| Metrica | Valor |
|---------|-------|
| Volume | 47 registros, 5 colunas |
| Granularidade | Anual x UF |
| `remuneracao_media` | **100% nula** (47/47 registros) |

### Cobertura por ano

| Ano | Formato FTP | Status | UFs coletadas |
|-----|------------|--------|---------------|
| 2015 | `{UF}2015.7z` | Parcial | AL, MA, RN, SE (5 UFs corrompidas) |
| 2016 | `{UF}2016.7z` | Parcial | 8 de 9 (PE corrompido) |
| 2017 | `{UF}2017.7z` | Parcial | 8 de 9 (BA corrompido) |
| 2018 | `RAIS_VINC_PUB_NORDESTE.7z` | Completo | 9/9 (11.5M registros) |
| 2019 | `RAIS_VINC_PUB_NORDESTE.7z` | Falhou | Download corrompido |
| 2020 | `RAIS_VINC_PUB_NORDESTE.7z` | Falhou | Download corrompido |
| 2021 | `RAIS_VINC_PUB_NORDESTE.7z` | Completo | 9/9 (12.1M registros) |
| 2022 | `RAIS_VINC_PUB_NORDESTE.7z` | Completo | 9/9 (13.5M registros) |
| 2023 | `RAIS_VINC_PUB_NORDESTE.7z` | Falhou | Formato `.COMT` (nao CSV) |

Como a **variavel dependente agora e emprego**, esta e a lacuna mais critica do dataset.

---

## 5. SICONFI (DCA + RREO + RGF) — Dados Fiscais

**Status:** MUITO BOM

| Relatorio | Registros | Periodo | Nulos |
|-----------|-----------|---------|-------|
| DCA | 232.661 | 2015-2024 | Zero nos valores |
| RREO | 1.987.525 | 2015-2025 | Zero |
| RGF | 95.760 | 2015-2025 | Zero |

Variaveis fiscais (investimento, resultado primario, divida) disponiveis conforme previsto na tese. DCA vai so ate 2024 (2025 sera publicado em meados de 2026).

---

## 6. Transferencias Constitucionais

**Status:** BOM

| Metrica | Valor |
|---------|-------|
| Volume | 42.205 registros |
| Periodo | 2015-2025, 9 UFs |
| Nulos | Zero |
| Outlier | `valor`: 65 registros (8.9%) fora do IQR |

Nota: `rotulo` possui 313 nulos (42.7%) na amostra CE.

---

## 7. Anomalias e Outliers Documentados

### 7.1 Alagoas — Sazonalidade extrema (ciclo sucroalcooleiro)

AL apresenta variabilidade 2.4x maior que SE na razao admissoes/desligamentos. Padrao sazonal recorrente em **setembro** (safra canavieira):

| Periodo | Razao adm/desl | Saldo |
|---------|----------------|-------|
| 2015-09 | 2.33 | +11.207 |
| 2016-09 | 3.03 | +13.395 |
| 2020-09 | 3.71 | +16.673 |
| 2024-09 | 2.14 | +15.420 |

**Conclusao:** Padrao real da economia alagoana, nao erro de dados.

### 7.2 Choque COVID — Abril/2020

Todos os estados com queda abrupta. Maiores impactos: BA (-35.098), CE (-31.085), PE (-26.212).

**Conclusao:** Outlier real (pandemia), nao erro de dados.

### 7.3 Sazonalidade geral do Nordeste

| Mes | Saldo medio | Interpretacao |
|-----|-------------|---------------|
| Set | +7.104 | Pico maximo |
| Ago | +5.346 | Pico preparatorio |
| Dez | -4.752 | Vale maximo (13o, rescisoes) |
| Mar | -1.110 | Destruicao |

---

## 8. Codificacao e Harmonizacao

### Campo `sexo`

| Codigo | Antigo | Novo | Significado |
|--------|--------|------|-------------|
| 1 | Sim | Sim | Masculino |
| 2 | Sim | - | Feminino |
| 3 | - | Sim (6.16M adm) | Feminino (eSocial) |

**Acao:** Mapeamento `{2: "F", 3: "F"}` aplicado na pipeline.

### Campo `grau_instrucao`

Zero-padding variavel entre anos (`"01"` vs `"1"`). Normalizado para inteiro na pipeline.

---

## 9. Participacao Regional

| UF | Saldo acumulado 2015-2025 | Participacao |
|----|---------------------------|-------------|
| BA | +407.516 | 25.6% |
| CE | +336.092 | 21.1% |
| PE | +219.739 | 13.8% |
| MA | +151.378 | 9.5% |
| PB | +134.553 | 8.4% |
| RN | +117.746 | 7.4% |
| AL | +89.552 | 5.6% |
| PI | +86.466 | 5.4% |
| SE | +49.583 | 3.1% |
| **NE** | **+1.592.625** | **100%** |

---

## 10. Checagens Automatizadas (Camada Quality — 19 datasets)

| Criterio | Descricao |
|----------|-----------|
| Existencia do arquivo | Verifica se a saida esperada foi gerada |
| Cobertura temporal | Identifica anos ausentes e intervalo observado |
| Cobertura territorial | Conta UFs presentes por dataset |
| Continuidade | Conta lacunas em combinacoes periodo x UF |
| Nulos totais | Percentual global de celulas vazias |
| Nulos em colunas criticas | Foco nas variaveis essenciais |
| Duplicidade por chave | Verifica repeticao em chaves esperadas |

### Resultados da ultima auditoria (quality_summary.csv)

| Dataset | Camada | Status | Registros | Missing% | Alertas |
|---------|--------|--------|-----------|----------|---------|
| bacen_raw_wide | raw | ok | 132 | 5.13% | 0 |
| caged_antigo_raw | raw | alerta | 423 | 0% | 117 lacunas periodo-UF |
| caged_novo_raw | raw | ok | 648 | 0% | 0 |
| rais_vinculos_raw | raw | alerta | 47 | 25% | 2019/2020 ausentes, remuneracao nula |
| siconfi_rreo_raw | raw | ok | 1.987.525 | 1.57% | 0 |
| siconfi_rgf_raw | raw | ok | 95.253 | 0.92% | 0 |
| siconfi_dca_raw | raw | alerta | 232.661 | 1.33% | 2025 ausente |
| transferencias_raw | raw | ok | 42.205 | 1.7% | 0 |
| bolsa_familia_* | raw | alerta | 0 | - | Opcional, nao encontrado |
| bacen_bimestral | processed | ok | 66 | 1.75% | 0 |
| caged_bimestral | processed | ok | 594 | 0% | 0 |
| painel_tese_bimestral | processed | ok | 594 | 9.69% | 0 |
| execucao_orcamentaria_* | processed | alerta | 0 | - | Opcionais, nao encontrados |

---

## 11. Resumo de Problemas e Acoes

| # | Sev. | Problema | Status |
|---|------|---------|--------|
| 1 | CRITICO | `salario_medio` = `total_movimentacoes` | **Corrigido** |
| 2 | ALTO | 13 meses ausentes no CAGED Antigo (FTP corrompido) | Pendente |
| 3 | ALTO | RAIS com gaps severos e `remuneracao_media` vazia | Pendente |
| 4 | ALTO | RAIS 2019, 2020 downloads corrompidos | Pendente |
| 5 | MEDIO | `sexo=3` no Novo = feminino (harmonizacao) | **Corrigido** |
| 6 | MEDIO | `grau_instrucao` inconsistente (zero-padding) | **Corrigido** |
| 7 | MEDIO | Bimestral herda lacunas (20% parciais) | Documentado |
| 8 | INFO | AL com sazonalidade extrema (set: razao >2x) | Documentado |
| 9 | INFO | COVID abr/2020 — saldo -35k (BA) | Documentado |

---

## 12. Dados Aptos para Uso

| Finalidade | Arquivo recomendado | Ressalvas |
|------------|---------------------|-----------|
| Painel econometrico completo | `model_ready/painel_tese_bimestral.csv` | 594 linhas, 37 colunas, 9.69% missing |
| Serie temporal de emprego (fluxo) | `model_ready/caged_bimestral.csv` | 13 bimestres parciais 2015-2019 |
| Series de credito deflacionadas | `model_ready/bacen_bimestral.csv` | Completo 2015-2025 |
| Analise setorial | `caged_por_setor.csv` | Apenas 2020-2025 |
| Analise por perfil | `caged_por_perfil.csv` + `caged_antigo_por_perfil.csv` | Harmonizar sexo e grau_instrucao |
| Salario medio | `caged_por_perfil.csv` / `caged_por_setor.csv` | **NAO usar** arquivos de saldo mensal |
| Obras/infraestrutura CE | `siof_obras_secretaria.csv` / `siof_obras_regiao.csv` | Apenas Ceara |
| Outliers de salario | Recomenda-se mediana ou winsorization (1%-99%) | PI out/2021 com R$ 1.9M |

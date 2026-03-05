# Relatório Analítico Detalhado: Qualidade dos Dados por Base

Esta análise detalha a qualidade de cada dataset considerando Granularidade, Completude, Consistência Temporal e Detecção de Outliers. As verificações focam na adequação dos dados para a modelagem da relação entre crédito e emprego.

## BACEN (Séries Macroeconômicas e Crédito - Nacional)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 132 registros e 13 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Data**.

### 2. Consistência Temporal
- **Período Coberto:** 2015-01 até 2025-12.
- **Continuidade:** Série temporal é contínua. Todos os 132 meses estão presentes, sem lacunas temporais (buracos na série).

### 3. Completude (Valores Ausentes)
- ⚠️ **Atenção:** `pib_trimestral_indice` possui 88 valores nulos (**66.7%** de falha de completude).

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
Identificados pontos fora da curva padrão (potenciais anomalias ou meses de choque econômico/pandemia):
  - **`IBCR_NE_ajuste_sazonal`**: 3 registros (2.3% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`ibc_br`**: 2 registros (1.5% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`inadimplencia_PF`**: 2 registros (1.5% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`ipca_mensal`**: 2 registros (1.5% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.

---
## BACEN Deflacionado (Valores Reais)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 132 registros e 18 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Data**.

### 2. Consistência Temporal
- **Período Coberto:** 2015-01 até 2025-12.
- **Continuidade:** Série temporal é contínua. Todos os 132 meses estão presentes, sem lacunas temporais (buracos na série).

### 3. Completude (Valores Ausentes)
- ⚠️ **Atenção:** `pib_trimestral_indice` possui 88 valores nulos (**66.7%** de falha de completude).

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
Identificados pontos fora da curva padrão (potenciais anomalias ou meses de choque econômico/pandemia):
  - **`IBCR_NE_ajuste_sazonal`**: 3 registros (2.3% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`ibc_br`**: 2 registros (1.5% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`inadimplencia_PF`**: 2 registros (1.5% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`ipca_mensal`**: 2 registros (1.5% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.

---
## CAGED Antigo (2015-2019)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 423 registros e 9 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Ano, Mês, Estado/UF**.

### 2. Consistência Temporal
- **Período Coberto:** Exercícios/Anos de 2015 a 2019.
- **Continuidade:** Série anual contínua, cobrindo perfeitamente 5 anos em sequência.

### 3. Completude (Valores Ausentes)
- ✅ **Dataset 100% preenchido.** Não foram detectados valores nulos (NaN) em nenhuma variável.

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
Identificados pontos fora da curva padrão (potenciais anomalias ou meses de choque econômico/pandemia):
  - **`saldo`**: 47 registros (11.1% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.

---
## Novo CAGED (2020-2025)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 648 registros e 9 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Ano, Mês, Estado/UF**.

### 2. Consistência Temporal
- **Período Coberto:** Exercícios/Anos de 2020 a 2025.
- **Continuidade:** Série anual contínua, cobrindo perfeitamente 6 anos em sequência.

### 3. Completude (Valores Ausentes)
- ✅ **Dataset 100% preenchido.** Não foram detectados valores nulos (NaN) em nenhuma variável.

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
Identificados pontos fora da curva padrão (potenciais anomalias ou meses de choque econômico/pandemia):
  - **`admissoes`**: 6 registros (0.9% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`desligamentos`**: 9 registros (1.4% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`saldo`**: 60 registros (9.3% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`salario_medio`**: 8 registros (1.2% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.
  - **`total_movimentacoes`**: 8 registros (1.2% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.

---
## RAIS (Vínculos Ativos)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 47 registros e 5 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Ano, Estado/UF**.

### 2. Consistência Temporal
- **Período Coberto:** Exercícios/Anos de 2015 a 2022.
- **Continuidade:** ⚠️ Foram encontradas lacunas temporais nos anos (presentes 6 de 8 esperados).

### 3. Completude (Valores Ausentes)
- 🚨 **CRÍTICO:** A coluna `remuneracao_media` possui 47 nulos (**100% da base**). Está completamente vazia.

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
- ✅ Nenhuma variação extrema (outliers severos) detectada nas variáveis numéricas de medição.

---
## Transferências Constitucionais (Amostra: CE)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 733 registros e 16 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Ano, Estado/UF, Conta/Rubrica, Período Fiscal**.

### 2. Consistência Temporal
- **Período Coberto:** Exercícios/Anos de 2015 a 2025.
- **Continuidade:** Série anual contínua, cobrindo perfeitamente 11 anos em sequência.

### 3. Completude (Valores Ausentes)
- ⚠️ **Atenção:** `rotulo` possui 313 valores nulos (**42.7%** de falha de completude).

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
Identificados pontos fora da curva padrão (potenciais anomalias ou meses de choque econômico/pandemia):
  - **`valor`**: 65 registros (8.9% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.

---
## SICONFI RREO (Amostra: CE)

**Status:** ✅ Carregado com sucesso.

### 1. Granularidade e Estrutura
- **Volume:** 330 registros e 8 colunas.
- **Granularidade:** Os dados estão dispostos ao nível de **Ano, Estado/UF, Conta/Rubrica, Período Fiscal**.

### 2. Consistência Temporal
- **Período Coberto:** Exercícios/Anos de 2015 a 2025.
- **Continuidade:** Série anual contínua, cobrindo perfeitamente 11 anos em sequência.

### 3. Completude (Valores Ausentes)
- ✅ **Dataset 100% preenchido.** Não foram detectados valores nulos (NaN) em nenhuma variável.

### 4. Distribuição e Outliers (Intervalo Interquartil - IQR)
Identificados pontos fora da curva padrão (potenciais anomalias ou meses de choque econômico/pandemia):
  - **`valor`**: 11 registros (3.3% da base) com valores estatisticamente extremos em relação ao padrão da distribuição.

---

# FASE 3: FLUXO DE DADOS — O que entra, sai e persiste?

## 1. Entradas de Dados (Origens)

### 1.1 BACEN-SGS (Séries Macroeconômicas)

**Fonte:** `pipeline/extract/bacen.py` — 106 linhas

```python
class BacenSGS:
    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}"
    
    SERIES_CODIGOS = {
        25389: "IBCR Nordeste",
        14084: "Saldo crédito PF - Nordeste",
        14089: "Saldo crédito PJ - Nordeste",
        4189: "SELIC mensal",
        433: "IPCA mensal",
        # [10 mais]
    }
```

**Protocolo:** REST API (GET)  
**Autenticação:** Nenhuma  
**Taxa:** ~0.5 req/s  
**Período:** 2015-2025 (mensal, contínuo)  
**Saída:** `dados_nordeste/raw/bacen/bacen.csv`

**Estrutura bruta:**
```
data,ibcr_ne,saldo_credito_pf_ne,saldo_credito_pj_ne,selic_mensal,ipca,...
2015-01-01,89.5,450000000,1200000000,13.65,0.31,...
2015-02-01,90.1,455000000,1210000000,13.25,0.35,...
...
2025-12-01,102.3,800000000,2100000000,9.50,0.25,...
```

**Status:** ✅ 100% OK — 132 registros, 13 colunas, zero nulos (exceto PIB trimestral esperado)

---

### 1.2 SICONFI (Receitas, Despesas, Dívida)

**Fonte:** `pipeline/extract/siconfi.py` — 178 linhas  
**API:** `https://apidatalake.tesouro.gov.br/ords/ddw/...`

**3 módulos:**
1. **RREO** (Receitas e Resultado Orçamentário)
   - Granularidade: Bimestral, por UF
   - Período: 2015-2025
   - Volume: 1.987.525 linhas brutas
   - Dados: receitas, despesas, resultado primário

2. **RGF** (Relatório de Gestão Fiscal)
   - Granularidade: Quadrimestral, por UF
   - Período: 2015-2025
   - Volume: 95.760 linhas
   - Dados: pessoal, dívida consolidada, passivos

3. **DCA** (Demonstrativo de Contas Anuais)
   - Granularidade: Anual, por UF
   - Período: 2015-2024
   - Volume: 232.661 linhas
   - Dados: ativo, passivo, patrimônio, investimento

**Protocolo:** REST API + Paginação (1.000 linhas/página)  
**Rate limit:** 5 req/s (implementado com sleep)  
**Saída:** `dados_nordeste/raw/siconfi_{rreo,rgf,dca}/`

**Status:** ✅ 100% OK — dados completospor período

---

### 1.3 CAGED e RAIS (Emprego Formal)

**Fonte:** `pipeline/extract/caged_rais.py` — 965 linhas (extrator mais complexo)  
**Protocolo:** FTP + descompressão .7z

#### 1.3.1 CAGED Antigo (2015-2019)

```
FTP: ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/{ano}/CAGEDEST_{MMAA}.7z
Arquivo por mês, comprimido com 7zip
```

**Volume esperado:** 423 arquivos (60 meses × 7 UFs)  
**Status:** ⚠️ 13 gaps identificados (7z corrompido ou deletado no servidor)

**Estrutura:**
```csv
cod_ibge,nome_uf,mes,ano,saldo_mensal,admissoes,demissoes,var_estoque
23,CE,01,2015,+15000,45000,30000,450000
...
```

#### 1.3.2 CAGED Novo (2020+)

```
FTP: ftp://ftp.mtps.gov.br/pdet/microdados/NOVO CAGED/{ano}/{anoMM}/CAGEDMOV{anoMM}.7z
Estrutura diferente (dados brutos do CAGED novo)
```

**Volume:** 648 meses (12 meses × ~54 anos entre 2020-2026)  
**Status:** ✅ 100% OK — dados contínuos

**Diferença:** CAGED Novo tem movimento diário (antes era saldo mensal), requer agregação.

#### 1.3.3 RAIS (Vínculo Formal — Estoque)

```
FTP: ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/{ano}/RAIS_VINC_PUB_NORDESTE.7z
Arquivo anual por UF (agregado do Nordeste)
```

**Volume:** 47 anos (2015-2022) + setorial  
**Status:** ⚠️ Gaps críticos em 2019-20 (downloads corrompidos)  
**Remuneração:** 100% nulo (campo não preenchido pelo MTE)

**Estrutura:**
```csv
ano,uf,cnae_divisao,ocupacao_cbo,vinculo_ativo,vinculo_total,remuneracao_media
2015,23,01,2310,450000,500000,NULL
...
```

**Protocolo:** FTP (timeout 120s), descompressão py7zr  
**Taxa:** Sob demanda (lento, ~10-30 min por CAGED completo)  
**Saída:** `dados_nordeste/raw/caged/` e `raw/rais/`

**Status:** ⚠️ 85% — frágil, FTP rate limits, 2019-20 inútil

---

### 1.4 Portal Transparência Brasil (Transferências)

**Fonte:** `pipeline/extract/portal_transparencia.py` — 181 linhas  
**Protocolo:** REST API (opcional com API key via env)

```python
PORTAL_TRANSPARENCIA_API_KEY = os.environ.get("PORTAL_TRANSPARENCIA_API_KEY")
```

**Capacidade:**
- Com API key: 1.000 req/dia, resultados mais rápidos
- Sem API key: fallback web scraping (lento, ~5 req/min)

**Período:** 2015-2025  
**Saída:** `dados_nordeste/raw/transferencias/`

**Status:** ⚠️ Sem key (fallback web), lento

---

### 1.5 Transparência Estadual (Alagoas, Piauí)

**Fontes:**
- `pipeline/extract/transparencia_al.py` — Web scraping
- `pipeline/extract/transparencia_pi.py` — Web scraping

**Protocolo:** HTTP + Selenium/BeautifulSoup (web scraping)  
**Taxa:** 1-2 req/s (respeitando servidor)  
**Período:** 2015-2025  
**Saída:** `dados_nordeste/raw/execucao_orcamentaria/al/` e `pi/`

**Status:** ⚠️ 85% — web frágil, sem API formal

---

### 1.6 SIOF-CE (Execução Orçamentária Ceará)

**Fonte:** `pipeline/extract/siof.py` — 354 linhas  
**Protocolo:** WebForm ASP.NET (scraping ViewState)

```python
# Ceará não tem API — usa form:
# POST https://siof.sefaz.ce.gov.br/
#   ViewState=[encoded], EventTarget=...
```

**Extração:**
1. GET forma (extrai ViewState)
2. POST formulário com parâmetros
3. Parse resposta HTML/tabelas
4. Extrai: obras, instalações, execução por secretaria/região

**Período:** Anual (dados 2015-2025)  
**Saída:** `dados_nordeste/raw/execucao_orcamentaria/ce/`

**Status:** 🔴 80% — ViewState pode quebrar com updates do servidor

---

## 2. Transformações Intermediárias

### ETL (raw → processed)

**Arquivo:** `pipeline/transform/etl.py` — 623 linhas

**Lógica por fonte:**

| Fonte | Raw input | Transformações | Processed output |
|-------|-----------|-----------------|------------------|
| **bacen** | `bacen.csv` (132 × 13) | Rename cols, types | `bacen/bacen.csv` |
| **siconfi_rreo** | `siconfi_rreo.csv` (1.9M) | Group by UF, reshape | `siconfi_rreo/{uf}/*.csv` |
| **caged** | `caged_antigo.csv` + `caged_novo.csv` | Unify format, saldo mensal | `caged/{uf}/caged_bimestral.csv` |
| **rais** | `rais_vinc.csv` (47K) | Filter vinculo_ativo, drop nulos | `rais/{uf}/rais_estoque.csv` |

**Exemplo de transformação:**

```python
# Entrada: bacen bruto (data, serie_1, serie_2, ...)
df_raw = pd.read_csv("raw/bacen/bacen.csv")

# Saída: bacen processado (data, ibcr_ne, credito_pf_ne, ...)
df_proc = df_raw.rename(columns={
    "serie_25389": "ibcr_ne",
    "serie_14084": "credito_pf_ne",
    ...
}).astype({
    "data": "datetime64",
    "ibcr_ne": "float64",
    ...
})

save_dataframe(df_proc, "processed/bacen/bacen.csv")
```

**Output:** 134 CSVs processados, 4.2 MB total

---

### Preparação para Modelo (processed → model_ready)

**Arquivo:** `pipeline/transform/preparacao_modelo.py` — 593 linhas

**Transformações:**

1. **Deflacionamento** — valores nominais → reais (base 2015)
   ```python
   # Índice IPCA mensal do BACEN
   df["valor_real"] = df["valor_nominal"] / (df["ipca_acumulado"] / 100)
   ```

2. **Harmonização temporal bimestral**
   - Resample dados mensais → bimestral (média ou soma conforme fonte)
   - Exemplo: BACEN mensal → bimestral (média)
   - Exemplo: CAGED mensal → bimestral (soma de admissões)

3. **Agregação por UF**
   - Merge de múltiplas fontes por (UF, período)
   - Resultado: painel único

**Output final:** `model_ready/painel_tese_bimestral.csv`

```
594 linhas (9 UFs × 66 bimestres de 2015-2025)
37 colunas: data, uf, credito_pf, credito_pj, ibcr, selic, ipca, ..., investimento_publico, divida, transferencias

Exemplo de linha:
2015-01-31,CE,450B,1200B,89.5,13.65,0.31,...,80M,500M,1200M
```

**Status de missing (9.69% total):**
- Concentrado em RAIS (gap 2019-20) e CAGED Antigo (13 gaps)
- IBCR-NE: 100% cobertura
- Transferências: 95% cobertura (BPC/Bolsa Familia parcial)

---

## 3. Auditoria de Qualidade

**Arquivo:** `pipeline/quality.py` — 616 linhas

**19 Verificações Automáticas:**

1. **Cobertura:** % registros por UF/período
2. **Nulos:** % missing, concentração por coluna
3. **Duplicidade:** linhas exatas duplicadas
4. **Tipos de dados:** int vs float vs string
5. **Ranges:** valores fora de limites esperados
6. **Série temporal:** continuidade, gaps
7. **RAIS:** remuneração (esperado 100% nulo)
8. **CAGED:** saldo < 0 (sinais esperados)
9. **SICONFI:** soma receitas = despesas?
10. [+9 mais]

**Output:**
```
dados_nordeste/quality/
├── quality_report.json    (estruturado)
├── quality_report.md      (markdown)
└── quality_summary.csv    (tabular)
```

**Exemplo de saída:**
```json
{
  "bacen": {
    "cobertura": "100%",
    "nulos_pct": 0.0,
    "duplicitas": 0,
    "status": "OK"
  },
  "rais": {
    "cobertura": "85%",
    "nulos_pct": 15.0,
    "remuneracao_nula_pct": 100.0,
    "status": "ALERTA: remuneração 100% nula"
  }
}
```

---

## 4. Persistência e Saídas

### 4.1 Dados Raw (Ignorados no Git)

```
dados_nordeste/raw/
├── bacen/nacional/bacen.csv              (~50 KB)
├── siconfi_rreo/ce/rreo_resumo.csv       (~100 KB)
├── siconfi_rgf/ce/rgf_resumo.csv         (~50 KB)
├── siconfi_dca/ce/dca_resumo.csv         (~40 KB)
├── caged/
│   ├── caged_antigo_saldo_mensal.csv    (~100 KB)
│   └── caged_novo_saldo_mensal.csv      (~80 KB)
├── rais/
│   ├── rais_vinculos.csv                (~30 KB)
│   └── rais_por_setor.csv               (~200 KB)
├── transferencias/                       (~400 KB)
└── execucao_orcamentaria/
    ├── ce/siof_*                         (~20 KB)
    ├── al/*                              (~10 KB)
    └── pi/*                              (~10 KB)

Status: .gitignore ignora raw/** — não commitado
Criação: Apenas via pipeline.run --full ou subconjunto
Retenção: Local apenas (sem backup automático)
```

### 4.2 Dados Processados (Commitados no Git)

```
dados_nordeste/processed/          (4.2 MB, commitado)
├── bacen/
│   └── bacen.csv                  (132 × 13, 60 KB)
├── caged/
│   ├── caged_bimestral.csv        (594 × 5, 50 KB) ← Principal emprego
│   ├── caged_bimestral_por_uf.csv
│   └── [caged detalhado por UF]
├── rais/
│   ├── rais_bimestral.csv
│   └── [rais por UF]
├── siconfi_rreo/
│   └── [resultado primário por UF]
├── siconfi_rgf/
│   └── [dívida consolidada por UF]
├── siconfi_dca/
│   └── [investimento público por UF]
├── transferencias/
│   └── [transferências por UF]
├── execucao_orcamentaria/
│   ├── ce/siof_consolidado.csv
│   ├── ce/siof_obras_secretaria.csv
│   ├── ce/siof_obras_regiao.csv
│   ├── al/*.csv
│   └── pi/*.csv
└── model_ready/                   (320 KB, painel final)
    ├── painel_tese_bimestral.csv  (594 × 37) ← TARGET
    ├── bacen_bimestral.csv
    ├── caged_bimestral.csv
    ├── resultado_primario_bimestral.csv
    ├── dcl_bimestral.csv
    ├── investimento_publico_bimestral.csv
    ├── transferencias_bimestrais.csv
    ├── rais_bimestral.csv
    └── matriz_regras_modelo.csv
```

**Retenção:** Permanente (commitado, versionado no Git)

### 4.3 Metadados de Execução

```
dados_nordeste/
├── metadata_coleta.json           (última execução)
├── logs/                          (arquivos .log por execução)
│   ├── pipeline_completa.log      (268 KB, último full run)
│   └── pipeline_fix.log           (105 KB)
└── coleta_status/                 (status timestamp)
    └── last_update.txt
```

**Exemplo metadata_coleta.json:**
```json
{
  "timestamp": "2026-04-15T12:33:00Z",
  "modulos_executados": ["bacen", "siconfi_rreo", "siconfi_rgf", "caged_rais"],
  "duracao_segundos": 1245,
  "resumo": {
    "bacen": 132,
    "siconfi_rreo": 1987525,
    "caged": 648,
    "status": "OK"
  }
}
```

### 4.4 Dashboard Streamlit (Frontend)

```
Frontend carrega via st.cache_data():
├── app.py               → Carrega CSV processados, exibe estatísticas
├── pages/1_BACEN_*.py   → Plotly: série temporal crédito
├── pages/10_CAGED_*.py  → Plotly: emprego por UF/período
└── [...]               → Cada página: carrega, filtra, plota
```

**Protocolo:** HTTP (Streamlit SSR)  
**Output:** HTML interativo (Plotly embed) + download CSV opcional

---

## 5. Fluxo Completo End-to-End

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. COLETA (pipeline.run --full)                                 │
│                                                                  │
│ BacenSGS.coletar() ──────────────┐                             │
│ Siconfi.coletar_*() ─────────────┼──→ dados_nordeste/raw/  │
│ CagedRais.coletar() ─────────────┤                             │
│ [...] ────────────────────────────┘                             │
│                                                                  │
│ Duração: ~2-3 horas (lento: FTP, rate limits STN)             │
│ Saída: ~600 MB raw (não commitado)                            │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. ETL (pipeline.transform.etl)                                 │
│                                                                  │
│ Rename, reshape, pivotagem por UF/período                      │
│ Output: dados_nordeste/processed/  (~134 CSVs, 4.2 MB)         │
│                                                                  │
│ Duração: ~5-10 minutos                                         │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. PREPARAÇÃO (pipeline.transform.preparacao_modelo)            │
│                                                                  │
│ Deflacionamento IPCA, harmonização bimestral                   │
│ Output: dados_nordeste/processed/model_ready/                  │
│          → painel_tese_bimestral.csv (594 × 37)               │
│                                                                  │
│ Duração: ~1 minuto                                             │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. AUDITORIA (pipeline.quality)                                │
│                                                                  │
│ 19 verificações automáticas                                    │
│ Output: dados_nordeste/quality/{json,md,csv}                   │
│                                                                  │
│ Duração: ~2 minutos                                            │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. COMMIT & DEPLOY (Git + Coolify)                             │
│                                                                  │
│ $ git add dados_nordeste/processed/                            │
│ $ git add dados_nordeste/quality/                              │
│ $ git commit -m "data: update processed data"                 │
│ $ git push origin main                                         │
│                                                                  │
│ Coolify webhook → Docker build → streamlit run                │
│ Novo painel disponível em http://VPS:8501/                    │
│                                                                  │
│ Duração: ~5 minutos                                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Anomalias e Riscos no Fluxo

| Anomalia | Localização | Severidade | Impacto |
|----------|-------------|-----------|---------|
| **RAIS sem remuneração** | `raw/rais/*.csv` | 🔴 Alta | Model não pode usar como proxy salarial |
| **CAGED Antigo 13 gaps** | `raw/caged/caged_antigo_*.csv` | 🔴 Alta | 2015-2019 com lacunas, afeta série temporal |
| **SIOF apenas 3 UFs** | `processed/execucao_orcamentaria/` | 🟡 Média | 6 UFs sem dados (PE, RN, PB, MA, BA, SE) |
| **FTP rate limits** | Pipeline extract (CAGED, RAIS) | 🟡 Média | Execução aleatoriamente falha após 30 min |
| **WebForm ASP.NET frágil** | `siof.py` ViewState scraping | 🟡 Média | Quebra com update servidor (ViewState muda) |
| **Sem API key Portal** | `portal_transparencia.py` | 🟡 Média | Fallback lento (web scraping), ~5 req/min |
| **SICONFI rate limit** | `siconfi.py` | 🟠 Baixa | Implementado com sleep(0.2), ~OK |

---

## 7. Conclusão da Fase 3

**Fluxo:** Bem documentado e lógico (Extract → Transform → Quality → Deploy)  
**Persistência:** Dados processados em Git (4.2 MB), raw local (~600 MB)  
**Integrações:** 9 fontes, 6 frágeis (FTP, web scraping)  
**Status:** Operacional, mas com riscos em coletores  
**Próximo:** Fase 4 — Saúde do Código


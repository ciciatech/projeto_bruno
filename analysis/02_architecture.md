# FASE 2: ARQUITETURA — Como está organizado?

## Diagrama Lógico de Camadas

```
┌─────────────────────────────────────────────────────────────┐
│                    FRONTEND: Streamlit                       │
│                                                               │
│   app.py (Home) → pages/{1..10} (10 dashboards temáticos)  │
│                                                               │
│   Tecnologia: Streamlit 1.54.0 (SSR Python → HTML)          │
│   Deployment: Docker Container (8501)                        │
│   Público: Aberto (sem auth)                                │
└────────┬────────────────────────────────────────────────────┘
         │
         │ Carrega CSVs via st.cache_data()
         │
         ├──────────────────────────────────────────────────────────┐
         │                                                            │
┌────────▼────────────────────────────────────────────────────┐   │
│                 CAMADA DE DADOS: Disk-Based                 │   │
│                                                               │   │
│ dados_nordeste/processed/                                   │   │
│ ├── bacen/                   (60 KB)                        │   │
│ ├── caged/                   (396 KB)   ← Emprego principal │   │
│ ├── siconfi_*/               (2.6 MB)   ← Fiscal            │   │
│ ├── transferencias/          (1.2 MB)   ← Políticas sociais │   │
│ ├── execucao_orcamentaria/   (52 KB)    ← Obras (3 UFs)     │   │
│ └── model_ready/             (320 KB)   ← Painel final      │   │
│                                                               │   │
│ Formato: CSV (não in-memory, lido por página)              │   │
│ Tamanho: 4.2 MB total (commitado no git)                   │   │
│ Update: Manual (pipeline.run --etl rodado manualmente)     │   │
└────────┬────────────────────────────────────────────────────┘   │
         │                                                           │
         │ (Python lê, transforma, retorna para página)            │
         │                                                           │
┌────────▼────────────────────────────────────────────────────┐   │
│              PIPELINE DE ETRAÇÃO (Raw ← APIs)               │   │
│                                                               │   │
│ pipeline/extract/                                           │   │
│ ├── bacen.py              → BACEN-SGS (REST API)            │   │
│ ├── siconfi.py            → STN (REST API)                  │   │
│ ├── caged_rais.py         → MTE/PDET (FTP + 7z)            │   │
│ ├── transferencias.py     → Derivado (RREO)                │   │
│ ├── bolsa_familia.py      → SAGI/MDS + Transparência       │   │
│ ├── portal_transparencia.py → Central (multi-datasets)     │   │
│ ├── transparencia_al.py   → Alagoas                         │   │
│ ├── transparencia_pi.py   → Piauí                           │   │
│ └── siof.py               → SEPLAG/CE (WebForm ASP.NET)    │   │
│                                                               │   │
│ Output: dados_nordeste/raw/                                 │   │
│ Modo: Executado manualmente via pipeline.run()             │   │
└────────┬────────────────────────────────────────────────────┘   │
         │                                                           │
         │ (Lê raw, normaliza, serializa)                          │
         │                                                           │
┌────────▼────────────────────────────────────────────────────┐   │
│        CAMADA DE TRANSFORMAÇÃO (Raw → Processed)            │   │
│                                                               │   │
│ pipeline/transform/                                         │   │
│ ├── etl.py                  (623 linhas)                    │   │
│ │   └── Pivotagem, rename, harmonização temporal            │   │
│ │       Output: processed/*/                                │   │
│ │                                                            │   │
│ └── preparacao_modelo.py    (593 linhas)                    │   │
│     └── Deflacionamento (IPCA), harmonização bimestral      │   │
│         Output: processed/model_ready/                      │   │
│                                                               │   │
│ Lógica: 8 regras de agregação (MODEL_RULES)                │   │
└────────┬────────────────────────────────────────────────────┘   │
         │                                                           │
         │ (Auditoria de qualidade)                                │
         │                                                           │
┌────────▼────────────────────────────────────────────────────┐   │
│            AUDITORIA (19 verificações automáticas)          │   │
│                                                               │   │
│ pipeline/quality.py         (616 linhas)                    │   │
│ ├── Cobertura (% registros por UF/período)                 │   │
│ ├── Nulos (% missing, concentração)                        │   │
│ ├── Duplicidade (linhas duplicadas)                        │   │
│ ├── Consistência (ranges de valores, tipos)                │   │
│ └── Output: dados_nordeste/quality/{json,md,csv}           │   │
│                                                               │   │
└────────────────────────────────────────────────────────────────┘   │
         ▲                                                             │
         │ (Entrada: APIs públicas)                                   │
         │                                                             │
    FONTES EXTERNAS                                                   │
    ├── BACEN-SGS (API REST)                                         │
    ├── SICONFI/STN (API REST, rate limits 5req/s)                  │
    ├── MTE/PDET (FTP, 7z compresso)                                 │
    ├── Portal Transparência Brasil (API opcional + fallback web)    │
    ├── SAGI/MDS (API Bolsa Familia)                                 │
    └── Portais AL/PI (Web scraping)                                 │
                                                                       │
         ◀─────────────────────────────────────────────────────────────┘
```

---

## Estrutura de Dependências

### Dependências Externas (Coletores)

```
┌─ BACEN-SGS (API REST)
│  └─ 13 séries econômicas (IBCR-NE, SELIC, IPCA, crédito, inadimplência)
│     └─ Frequência: Mensal | Período: 2015-2025 | Status: 100% OK
│
├─ SICONFI/STN (API REST)
│  ├─ RREO (receitas, despesas, resultado primário) — 1.9M linhas
│  ├─ RGF (gestão fiscal, dívida) — 95K linhas
│  └─ DCA (balanço patrimonial, investimento) — 232K linhas
│     └─ Frequência: Bimestral/Trimestral/Anual | Período: 2015-2025 | Status: 100% OK
│
├─ MTE/PDET (FTP, arquivos 7z)
│  ├─ CAGED Antigo (2015-2019) — 423 meses ⚠️ 13 gaps
│  ├─ CAGED Novo (2020+) — 648 meses ✅ OK
│  └─ RAIS (2015-2022) — 47 anos ⚠️ 2019-20 gaps, sem remuneração
│     └─ Frequência: Mensal/Anual | Status: 85% (frágil, FTP rate limits)
│
├─ Portal Transparência Brasil (API + web scraping)
│  ├─ Bolsa Familia (API SAGI/MDS — alternativa)
│  ├─ Transferências (whitelist 36 contas via RREO)
│  └─ Órgãos federais (genérico)
│     └─ Frequência: Variável | Status: Fallback (sem API key)
│
├─ Portal Transparência Alagoas & Piauí (web scraping)
│  └─ Execução orçamentária estadual
│     └─ Status: 85% (web frágil, sem API)
│
└─ SIOF/SEPLAG-CE (WebForm ASP.NET + scraping)
   ├─ Obras + Instalações (SIOF-CE específico)
   └─ Execução orçamentária CE
      └─ Status: 80% (scraping ViewState ASP.NET é frágil)
```

### Dependências Internas (Python)

```
pipeline/
├── config.py                   ← Tudo importa daqui
│   └── BASE_DIR, RAW_DIR, PROCESSED_DIR, ESTADOS_NE, PERIODO_*
│
├── utils.py                    ← Helpers para tudo
│   ├── setup_logging()         → Configura logger
│   └── save_dataframe()        → Padronização CSV
│
├── run.py                      ← Orquestrador central
│   ├── Importa: extract.*, transform.*, quality.*
│   └── PipelineColeta.executar(modulos=[...])
│
├── extract/                    ← Coletores (1.700 linhas)
│   ├── bacen.py               (106 linhas)
│   ├── siconfi.py             (178 linhas)
│   ├── caged_rais.py          (965 linhas) ← Maior complexidade
│   ├── transferencias.py      (125 linhas)
│   ├── bolsa_familia.py       (113 linhas)
│   ├── portal_transparencia.py (181 linhas)
│   ├── siof.py                (354 linhas) ← Complexidade média
│   └── [...].py               (vários)
│
├── transform/                 ← Transformações (1.300 linhas)
│   ├── etl.py                 (623 linhas)
│   │   └── Importa: config.*, extract.*, utils
│   │
│   └── preparacao_modelo.py   (593 linhas)
│       └── Importa: config, etl, utils
│
└── quality.py                 ← Auditoria (616 linhas)
    └── Importa: config, utils
```

### Dependências de Dados (Fluxo)

```
Raw Data:
  bacen/               ← bacen.py  → Processed bacen/
  siconfi_rreo/        ← siconfi.py → Processed siconfi_rreo/
  siconfi_rgf/         ← siconfi.py → Processed siconfi_rgf/
  siconfi_dca/         ← siconfi.py → Processed siconfi_dca/
  caged/               ← caged_rais.py → Processed caged/
  rais/                ← caged_rais.py → Processed rais/
  transferencias/      ← (derivada) → Processed transferencias/
  execucao_orcamentaria/ ← transparencia_*.py + siof.py

                        ↓ ETL (etl.py)

Processed:
  bacen/, caged/, siconfi_*/, transferencias/, execucao_orcamentaria/

                        ↓ Preparação (preparacao_modelo.py)

Model-Ready:
  painel_tese_bimestral.csv (594 linhas, 37 colunas) ← OBJETIVO FINAL
  bacen_bimestral.csv
  caged_bimestral.csv
  resultado_primario_bimestral.csv
  [...]
```

---

## Pontos de Entrada (Entry Points)

### 1. **app.py** (Frontend Streamlit)
**Responsabilidade:** Página raiz e navegação

```python
st.set_page_config(page_title="Dados Nordeste - Tese DESP/UFC", ...)
st.title("Dados Públicos — Nordeste (2015–2025)")

# Carrega lista de datasets e conta registros
datasets = {
    "BACEN Indicadores": {"caminhos": [...], "desc": "..."},
    "SICONFI RREO": {...},
    ...
}
```

**Import stack:**
```
app.py
  └─ from pipeline.config import PROCESSED_DIR
     └─ Lê: dados_nordeste/processed/**/*.csv
```

**Atividade:** Executado continuamente (Streamlit = stateless server)

---

### 2. **pipeline/run.py** (CLI da Pipeline)

**Modos de invocação:**

```bash
# Modo 1: Coleta rápida (apenas BACEN)
python3 -m pipeline.run --apenas-bacen

# Modo 2: Coleta principal (sem CAGED/RAIS)
python3 -m pipeline.run

# Modo 3: Subconjunto de módulos
python3 -m pipeline.run --modulos bacen siconfi_rreo transferencias

# Modo 4: Fluxo completo (coleta + ETL + quality)
python3 -m pipeline.run --full

# Modo 5: Executado via Docker (Coolify) — assumimos --full ou subconjunto
docker-compose up --build
```

**Execução esperada:** Manual (não automatizado, sem cron)

---

### 3. **pages/{1..10}/*.py** (Dashboards temáticos)

Cada página é Streamlit autossuficiente:

```
pages/
├── 1_BACEN_Indicadores.py     (série temporal: crédito, IBCR, SELIC, IPCA)
├── 2_Bolsa_Familia.py         (transferências federais por UF/tempo)
├── 3_SICONFI_RREO.py          (receitas e despesas)
├── 4_SICONFI_RGF.py           (gestão fiscal)
├── 5_SICONFI_DCA.py           (balanço patrimonial)
├── 6_Transferencias.py        (constitucionais: FPE, FUNDEB)
├── 7_SIOF_CE.py               (execução orçamentária CE + obras)
├── 8_Transparencia_AL.py      (Alagoas)
├── 9_Transparencia_PI.py      (Piauí)
└── 10_CAGED_RAIS.py           (emprego: CAGED + RAIS + estoque)
```

Cada página:
- Carrega CSVs via `st.cache_data()`
- Aplica filtros (UF, período)
- Plotia com Plotly
- Exporta CSV opcional

---

## Padrões Arquiteturais Identificados

### ✅ Padrão 1: Extrator em Camadas
Cada coletor segue:
```python
class ColetorX:
    @staticmethod
    def coletar_...():
        # 1. Conecta à fonte
        # 2. Itera sobre períodos/UFs
        # 3. Normaliza (rename, tipos)
        # 4. Retorna DataFrame
        return df

# Uso
df = ColetorX.coletar_...()
save_dataframe(df, "dados_nordeste/raw/...")
```

### ✅ Padrão 2: Orquestração Condicional (run.py)
```python
if "bacen" in modulos:
    df = BacenSGS.coletar_todas()
    resumo["bacen"] = len(df)
# Repete para cada módulo
```

### ✅ Padrão 3: ETL Modular
```python
# etl.py
def transformar_bacen(df_raw):
    return df_raw.rename(...).astype(...)

# (por fonte)
def transformar_caged(df_raw):
    return df_raw[colunas_esperadas].drop_duplicates()
```

### ⚠️ Padrão 4: Config Centralizada
```python
# config.py
BASE_DIR = Path("./dados_nordeste")
ESTADOS_NE = {"AL": {...}, ...}
PERIODO_INICIO = 2015
```
**Risco:** Mudar config requer reimplementação de tudo. Sem env vars.

---

## Integrações Externas

| Sistema | Protocolo | Autenticação | Fallback | Status |
|---------|-----------|--------------|----------|--------|
| BACEN-SGS | REST API | Nenhuma | Nenhum | ✅ OK |
| SICONFI/STN | REST API | Nenhuma (rate limit via CPF) | Nenhum | ✅ OK |
| MTE/PDET | FTP | Anônimo | Nenhum | ⚠️ Frágil |
| Portal Transparência BR | REST API + Web | Opcional (API key env) | Web scraping | ⚠️ Sem key |
| SAGI/MDS Bolsa Familia | REST API | Nenhuma | Portal Transparência | ⚠️ Lento |
| SEPLAG/CE SIOF | WebForm ASP | Nenhuma (scraping) | Nenhum | 🔴 Frágil |

---

## Dependências do Streamlit (Frontend)

```
streamlit==1.54.0
  ├─ pandas==2.3.3         (leitura CSV, processamento)
  ├─ plotly==6.5.2         (gráficos interativos)
  ├─ pyarrow==23.0.1       (serialização, cache)
  ├─ requests==2.32.5      (HTTP — não usado no app.py, mas em pages)
  ├─ tqdm==4.67.3          (barras de progresso)
  ├─ scipy>=1.11.0         (estatística — importado onde?)
  └─ py7zr>=0.20.0         (descompressão — usado em extract/, não em frontend)
```

**Observação:** Backend (extract) e Frontend (Streamlit) compartilham `requirements.txt`. No Docker, instala tudo.

---

## Conclusão da Fase 2

**Arquitetura:** 3 camadas bem definidas (Extract → Transform → Frontend)  
**Modularidade:** Alta (19 módulos Python independentes)  
**Acoplamento:** Baixo (config.py é o "pivot")  
**Escalabilidade:** Limitada (FTP frágil, sem caching, sem DB)  
**Risco arquitetural:** Médio (dependências externas frágeis, scrapy ASP.NET)

---

**Próximo:** Fase 3 — Fluxo de Dados


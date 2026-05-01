# FASE 4: SAÚDE DO CÓDIGO — Onde dói?

## Hotspots Identificados

### 🔴 CRÍTICO: Instabilidade de Coletores FTP

**Arquivo:** `pipeline/extract/caged_rais.py` (965 linhas)  
**Problema:** FTP downloads com rate limits, timeouts, 7z corruption

```python
# Linha 45-60: _ftp_download() sem retry exponencial
def _ftp_download(remote_path: str, local_path: str):
    logger.info(f"FTP download: {FTP_HOST}{remote_path}")
    with FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)  # ← Pode falhar silenciosamente
```

**Risco:** 
- Sem retry logic → falha aleatória
- Timeout 120s insuficiente para arquivos >200MB
- Sem validação de checksum após download
- 13 gaps em CAGED Antigo = possível morte anterior (FTP file gone)

**Impacto:** Série temporal emprego (variável dependente) com lacunas indetectáveis em produção

**Recomendação:** Implementar exponential backoff + checksum + fallback cache local

---

### 🔴 CRÍTICO: WebForm ASP.NET Scraping (SIOF)

**Arquivo:** `pipeline/extract/siof.py` (354 linhas)  
**Problema:** ViewState scraping é frágil a atualizações servidor

```python
# Linha 120-150: Extração de ViewState (ASP.NET)
def _extrair_viewstate(html_resposta: str) -> str:
    import re
    # Busca padrão fixo — quebra se SEPLAG/CE renomear campo
    match = re.search(r'id="__VIEWSTATE" value="([^"]+)"', html_resposta)
    if not match:
        raise ValueError("ViewState não encontrado — servidor alterou HTML")
```

**Risco:**
- Sem fallback quando padrão muda
- Sem alertas de quebra
- Lógica POST de formulário também pode quebrar

**Impacto:** Execução orçamentária CE desaparece de uma noite para outra

**Recomendação:** Implementar detecção de padrão e fallback API simulado ou Selenium headless

---

### 🟡 ALTO: Harmoni ização Temporal Sem Validação Semântica

**Arquivo:** `pipeline/transform/preparacao_modelo.py` (593 linhas)  
**Problema:** Resample mensal → bimestral sem verificação de lógica econômica

```python
# Linha 200-220: Agregação bimestral
df_bim = df_mensal.resample('2M').agg({
    'bacen_credito': 'mean',      # ← Média de crédito?
    'caged_admissoes': 'sum',      # ← Soma de admissões OK
    'rais_estoque': 'mean',        # ← Média de estoque? Deveria ser último valor
    'siconfi_resultado': 'sum',    # ← Soma de resultado?
})
```

**Risco:**
- Média de crédito mensal ≠ crédito bimestral (stock vs flow)
- Média de estoque emprego ≠ estoque bimestral
- Sem documentação de regras aplicadas

**Impacto:** Modelo econométrico pode estar using valores semanticamente incorretos

**Recomendação:** Criar matriz de agregação (`matrix_regras_modelo.csv`) e validá-la contra tese

---

### 🟡 ALTO: RAIS Completamente Nula (Remuneração)

**Arquivo:** `pipeline/extract/caged_rais.py` — Linhas 600-650  
**Problema:** Campo `remuneracao_media` 100% nulo no MTE FTP

```csv
ano,uf,remuneracao_media
2015,23,
2015,24,
2016,23,
```

**Risco:**
- Não pode ser usado como proxy salarial
- Não pode validar "R$ 1bi em crédito → X empregos com Y salário"
- Documentação não menciona essa limitação no Modelo

**Impacto:** Proxy de informalidade via RAIS inviável

**Recomendação:** Buscar alternativa (RAIS 2.0 em PDET, ou SECEX/CNAE dados externos)

---

### 🟡 ALTO: Sem Testes Automatizados

**Status:** 0 testes identificados  
**Arquivos:** Não há `tests/` ou `test_*.py`  
**Impacto:** Quebra de API silenciosa só detectada em produção

**Exemplo de teste crítico:** "Se SICONFI está up, dados devem chegar"

---

### 🟡 MÉDIO: Documentação Inline Parcial

**Padrão:** Docstrings em alguns módulos, nada em outros

| Arquivo | Docstrings | Status |
|---------|-----------|--------|
| `bacen.py` | ✅ Sim | OK |
| `caged_rais.py` | ❌ Não | Risco |
| `etl.py` | ⚠️ Parcial | Função de agregar não documentada |
| `preparacao_modelo.py` | ⚠️ Parcial | Regras de deflacionamento não claras |

**Impacto:** Onboarding lento, bug fixes no código errado

---

### 🟡 MÉDIO: Logging Assimétrico

**Status:** `setup_logging()` em `utils.py` cria logger, mas uso inconsistente

```python
logger.info("Starting download...")     # pipeline/extract/*.py
logger.warning("...")                   # pipeline/run.py
# Nada em pages/*.py — Streamlit tem seu próprio logger
```

**Risco:** Difícil debugar falhas em produção (Coolify logs)

---

## Métricas de Código

| Métrica | Valor | Avaliação |
|---------|-------|-----------|
| **Total de linhas** | 9.136 | Médio |
| **Maior arquivo** | caged_rais.py (965) | Alto |
| **Média por arquivo** | 480 | Razoável |
| **Complexidade ciclomática** (inferida) | ~4-8 por função | Baixa-média |
| **Duplicação de código** (estimada) | 10-15% | Presente |
| **Cobertura de testes** | 0% | Crítico |
| **Documentação (docstrings)** | 40% | Baixo |

---

## Dívida Técnica Identificada

| Item | Localização | Prioridade | Esforço | ROI |
|------|-----------|-----------|---------|-----|
| Implementar retry com backoff exponencial | `caged_rais.py` | 🔴 Alta | 2h | Alto |
| Extrair lógica SIOF para Selenium | `siof.py` | 🔴 Alta | 4h | Médio |
| Criar testes pytest básicos | `tests/` (novo) | 🟡 Média | 8h | Alto |
| Validar regras de agregação com economista | `preparacao_modelo.py` | 🟡 Média | 4h | Alto |
| Documentar cada coletor (docstrings) | `extract/*.py` | 🟡 Média | 6h | Médio |
| Buscar alternativa para RAIS salarial | `extract/caged_rais.py` | 🟡 Média | 8h | Alto |
| Centralizar logging (não Streamlit) | Vários | 🟠 Baixa | 2h | Baixo |

**Total dívida estimada:** 34h (5 dias)

---

## Padrões Problemáticos

### ❌ Padrão 1: Configuração Hardcoded

```python
# config.py linha 50
REQUEST_TIMEOUT = 60  # Deveria ser env var
MAX_RETRIES = 3       # Deveria ser env var
```

### ❌ Padrão 2: Importações Globais Sem Try-Except

```python
# extract/siof.py linhas 1-20
import selenium  # Pode não estar instalado
```

### ✅ Padrão 3: Bom — Funções Puras em Transform

```python
# transform/etl.py
def transformar_bacen(df_raw: pd.DataFrame) -> pd.DataFrame:
    return df_raw.rename(...).astype(...)  # Sem side effects
```

---

## Conclusão da Fase 4

**Saúde geral:** Média (código funcional, mas frágil em pontos críticos)

**Top 3 hotspots a corrigir:**
1. Retry logic FTP + SIOF scraping
2. Testes automatizados (crítico para CI/CD)
3. Validação de regras de agregação

**Próximo:** Fase 5 — Riscos


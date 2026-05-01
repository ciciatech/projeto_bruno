"""
Configuração centralizada do pipeline de coleta de dados.

Constantes, paths e parâmetros compartilhados por todos os módulos.
"""

from pathlib import Path

# ==============================================================================
# DIRETÓRIOS
# ==============================================================================

BASE_DIR = Path("./dados_nordeste")
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"
QUALITY_DIR = BASE_DIR / "quality"

# ==============================================================================
# ESTADOS DO NORDESTE
# ==============================================================================

ESTADOS_NE = {
    "AL": {"nome": "Alagoas", "cod_ibge": "27", "cod_siafi": "27"},
    "BA": {"nome": "Bahia", "cod_ibge": "29", "cod_siafi": "29"},
    "CE": {"nome": "Ceará", "cod_ibge": "23", "cod_siafi": "23"},
    "MA": {"nome": "Maranhão", "cod_ibge": "21", "cod_siafi": "21"},
    "PB": {"nome": "Paraíba", "cod_ibge": "25", "cod_siafi": "25"},
    "PE": {"nome": "Pernambuco", "cod_ibge": "26", "cod_siafi": "26"},
    "PI": {"nome": "Piauí", "cod_ibge": "22", "cod_siafi": "22"},
    "RN": {"nome": "Rio Grande do Norte", "cod_ibge": "24", "cod_siafi": "24"},
    "SE": {"nome": "Sergipe", "cod_ibge": "28", "cod_siafi": "28"},
}

# Mapa UF -> Nome (atalho usado pelo ETL e dashboard)
UF_NOMES = {uf: info["nome"] for uf, info in ESTADOS_NE.items()}

# ==============================================================================
# PERÍODO DE COLETA
# ==============================================================================

PERIODO_INICIO = 2015
PERIODO_FIM = 2025

# ==============================================================================
# ESCOPO DA PIPELINE REGIONAL CE
# ==============================================================================
# A partir da reformulação do exercício empírico (Prof. Paulo, abr/2026), o
# foco passa a ser o Ceará em granularidade município → 14 regiões de
# planejamento da SEPLAG-CE. Coletores NE legados continuam funcionais; novos
# coletores devem respeitar ESCOPO_CE.

ESCOPO_CE = True
UF_FOCO = "CE"
COD_IBGE_CE = "23"  # 2 dígitos
PERIODO_INICIO_MENSAL = 2015
# 2026 incluído porque o SIOF-CE da SEPLAG publica anualmente o orçamento
# corrente; mantê-lo no painel permite ao frontend exibir o ano vigente
# mesmo que outras coletas (BF/BPC/CAGED) ainda não cubram o ano todo.
PERIODO_FIM_MENSAL = 2026

# ==============================================================================
# PARÂMETROS DE REQUEST
# ==============================================================================

REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos

# ==============================================================================
# CHAVES DE API EXTERNAS (lidas de env var quando necessário)
# ==============================================================================
# - PORTAL_TRANSPARENCIA_API_KEY: cadastro gratuito em
#   https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
#   Requerida pelos coletores Bolsa Família, BPC e investimento federal.

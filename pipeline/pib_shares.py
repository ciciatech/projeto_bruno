"""
Shares anuais de PIB usados como hipóteses na construção do investimento privado
e do rateio do investimento federal não-identificado por estado.

Conforme metodologia do Prof. Paulo (áudio abr/2026):

- ``share_ce_brasil`` (~2,2%) — usado para mensalizar o FBCF Brasil para o CE,
  e para ratear investimento federal sem estado nem região identificados.
- ``share_ce_nordeste`` (~14,5%) — usado para ratear investimento federal
  identificado como Nordeste mas sem estado.

Fonte: IBGE Contas Regionais (tabela 5938 do SIDRA). Atualizar
``pipeline/data/pib_shares_ce.csv`` quando IBGE publicar novos anos.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PIB_SHARES_CSV = Path(__file__).parent / "data" / "pib_shares_ce.csv"

# Defaults caso o ano consultado seja anterior ou posterior ao CSV
DEFAULT_SHARE_CE_BR = 0.022
DEFAULT_SHARE_CE_NE = 0.145


@lru_cache(maxsize=1)
def _carregar() -> pd.DataFrame:
    if not PIB_SHARES_CSV.exists():
        logger.warning(f"PIB shares: {PIB_SHARES_CSV} ausente — usando defaults")
        return pd.DataFrame(
            columns=["ano", "share_ce_brasil", "share_ce_nordeste", "fonte"]
        )
    return pd.read_csv(PIB_SHARES_CSV)


def share_ce_brasil(ano: int) -> float:
    df = _carregar()
    row = df[df["ano"] == ano]
    if row.empty:
        if not df.empty:
            # Forward-fill: usar último ano disponível
            ult = df.iloc[-1]
            return float(ult["share_ce_brasil"])
        return DEFAULT_SHARE_CE_BR
    return float(row.iloc[0]["share_ce_brasil"])


def share_ce_nordeste(ano: int) -> float:
    df = _carregar()
    row = df[df["ano"] == ano]
    if row.empty:
        if not df.empty:
            ult = df.iloc[-1]
            return float(ult["share_ce_nordeste"])
        return DEFAULT_SHARE_CE_NE
    return float(row.iloc[0]["share_ce_nordeste"])


def shares_completos() -> pd.DataFrame:
    """Retorna DataFrame completo (ano, share_ce_brasil, share_ce_nordeste)."""
    return _carregar().copy()

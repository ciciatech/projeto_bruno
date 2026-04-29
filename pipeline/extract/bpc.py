"""
Coletor BPC — Benefício de Prestação Continuada por município CE.

Consome o CSV bulk público do Portal da Transparência. O BPC tem programa
estável (sem transição como o BF) — endpoint único ``/bpc/AAAAMM``.

Saída: ``dados_nordeste/processed/bpc/bpc_municipio_ce_mensal.csv`` com
(cod_ibge, regiao_codigo, regiao_nome, ano, mes, valor_total, beneficiarios).
"""

from __future__ import annotations

import logging

import pandas as pd

from pipeline.config import PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL
from pipeline.extract._portal_transp_csv import coletar_programa_mensal
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)


def coletar_ce(
    ano_inicio: int = PERIODO_INICIO_MENSAL,
    ano_fim: int = PERIODO_FIM_MENSAL,
    manter_zip: bool = False,
    pular_meses_existentes: bool = True,
) -> pd.DataFrame:
    """Coleta BPC mensal por município CE no período."""
    df = coletar_programa_mensal(
        nome_programa="bpc",
        url_path="bpc",
        ano_inicio=ano_inicio,
        ano_fim=ano_fim,
        manter_zip=manter_zip,
        pular_meses_existentes=pular_meses_existentes,
    )
    if df.empty:
        logger.warning("BPC: nenhum dado coletado.")
        return df

    save_dataframe(
        df,
        "bpc_municipio_ce_mensal",
        subdir="processed",
        path_parts=["bpc"],
    )
    logger.info(
        f"BPC consolidado: {len(df)} linhas, {df['cod_ibge'].nunique()} municípios, "
        f"{df.groupby(['ano','mes']).ngroups} meses."
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

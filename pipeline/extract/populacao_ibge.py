"""
Estimativas populacionais por município CE — IBGE / SIDRA tabela 6579.

Habilita os filtros "Per capita" no frontend e a normalização de variáveis
em modelos de painel.

Endpoint: https://servicodados.ibge.gov.br/api/v3/agregados/6579/...
Cobertura: 2001-presente, anual. Para 2010 e 2022 usa-se o Censo;
demais anos são estimativas (Lei 8.443/92, art. 102).

Saída:
  dados_nordeste/processed/populacao/populacao_ce_anual.csv
  com colunas (cod_ibge, regiao_codigo, regiao_nome, ano, populacao)
"""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd

from pipeline.config import PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL
from pipeline.regioes_ce import get_regiao_info
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)

BASE = "https://servicodados.ibge.gov.br/api/v3/agregados/6579"
VARIAVEL = "9324"  # População residente estimada


def _coletar_ano(ano: int) -> dict[str, int]:
    """Retorna {cod_ibge_7: populacao} para todos os municípios CE no ano."""
    url = (
        f"{BASE}/periodos/{ano}/variaveis/{VARIAVEL}"
        f"?localidades=N6[N3[23]]"  # N6 = município, N3[23] = filhos do estado CE
    )
    resp = safe_request(url, timeout=60)
    if resp is None:
        logger.warning(f"populacao IBGE: falhou para {ano}")
        return {}

    data = resp.json()
    if not data:
        return {}

    out: dict[str, int] = {}
    series = data[0].get("resultados", [])[0].get("series", [])
    for s in series:
        cod = str(s["localidade"]["id"]).zfill(7)
        valor = s["serie"].get(str(ano))
        if valor and valor != "...":
            try:
                out[cod] = int(valor)
            except (TypeError, ValueError):
                pass
    return out


def coletar_ce(anos: Iterable[int] | None = None) -> pd.DataFrame:
    anos = list(anos) if anos else list(range(PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL + 1))
    info = get_regiao_info()[["cod_ibge", "regiao_codigo", "regiao_nome"]]

    linhas: list[dict] = []
    for ano in anos:
        pop_map = _coletar_ano(ano)
        for cod, pop in pop_map.items():
            linhas.append({"cod_ibge": cod, "ano": ano, "populacao": pop})

    if not linhas:
        logger.warning("populacao IBGE: nenhum dado coletado")
        return pd.DataFrame()

    df = pd.DataFrame(linhas)
    df = df.merge(info, on="cod_ibge", how="inner")
    df = df[["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "populacao"]].sort_values(
        ["ano", "regiao_codigo", "cod_ibge"]
    ).reset_index(drop=True)

    save_dataframe(df, "populacao_ce_anual", subdir="processed", path_parts=["populacao"])
    logger.info(
        f"populacao IBGE: {len(df)} linhas, {df['cod_ibge'].nunique()} munic., "
        f"{df['ano'].nunique()} anos"
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

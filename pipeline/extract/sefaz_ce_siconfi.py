"""
Cota-Parte ICMS/IPVA municipal via SICONFI RREO Anexo 03.

Substitui o adapter manual do Ceará Transparente (que está bloqueado por
WAF) — extrai os mesmos dados a partir do Demonstrativo da Receita
Corrente Líquida (RREO Anexo 03) que cada prefeitura cearense já reporta
ao SICONFI.

Estratégia: o RREO Anexo 03 traz a série mensal dos últimos 12 meses
nas colunas <MR> (Mês de Referência), <MR-1>, ..., <MR-11>. Pegamos
o anexo do bimestre 6 (último) que cobre janeiro→dezembro do ano. Volume:
184 munic × 11 anos × 1 bimestre ≈ 2.024 requests, ~10 min.

Output:
  dados_nordeste/processed/sefaz_ce_siconfi/transf_estaduais_ce_mensal.csv
  com (cod_ibge, regiao_codigo, regiao_nome, ano, mes, icms, ipva, total)
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    def tqdm(it, **k):
        return it

from pipeline.config import PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL
from pipeline.extract.siconfi import Siconfi
from pipeline.regioes_ce import get_regiao_info, get_codigos_municipios_ce
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

ANEXO = "RREO-Anexo 03"
CONTAS = {"Cota-Parte do ICMS": "icms", "Cota-Parte do IPVA": "ipva"}
THROTTLE_S = 0.5

MR_TO_MES = {
    "<MR>": 12,
    "<MR-1>": 11, "<MR-2>": 10, "<MR-3>": 9, "<MR-4>": 8,
    "<MR-5>": 7, "<MR-6>": 6, "<MR-7>": 5, "<MR-8>": 4,
    "<MR-9>": 3, "<MR-10>": 2, "<MR-11>": 1,
}


def _coletar_um_municipio_ano(cod_ibge: str, ano: int) -> list[dict]:
    raw = Siconfi.coletar_rreo(ano, 6, cod_ibge, "CE")
    if raw.empty:
        return []
    sub = raw[(raw["anexo"] == ANEXO) & (raw["conta"].isin(CONTAS.keys()))]
    if sub.empty:
        return []

    por_mes: dict[int, dict[str, float]] = {}
    for _, row in sub.iterrows():
        col = row["coluna"]
        if col not in MR_TO_MES:
            continue
        mes = MR_TO_MES[col]
        campo = CONTAS[row["conta"]]
        valor = float(row["valor"])
        por_mes.setdefault(mes, {})[campo] = valor

    return [
        {
            "ano": ano,
            "mes": mes,
            "icms": dados.get("icms", 0.0),
            "ipva": dados.get("ipva", 0.0),
        }
        for mes, dados in por_mes.items()
    ]


def coletar_ce(
    anos: Iterable[int] | None = None,
    municipios: Iterable[str] | None = None,
    throttle: float = THROTTLE_S,
) -> pd.DataFrame:
    anos = list(anos) if anos else list(range(PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL + 1))
    municipios = list(municipios) if municipios else get_codigos_municipios_ce()
    info = get_regiao_info().set_index("cod_ibge")[["regiao_codigo", "regiao_nome"]]

    logger.info(
        f"SICONFI Cota-Parte ICMS/IPVA: {len(municipios)} munic x {len(anos)} anos = "
        f"{len(municipios) * len(anos)} requests"
    )

    linhas: list[dict] = []
    pbar = tqdm(total=len(municipios) * len(anos), desc="SICONFI cota-parte", unit="mun-ano")

    for cod_ibge in municipios:
        cod_ibge = str(cod_ibge).zfill(7)
        if cod_ibge not in info.index:
            pbar.update(len(anos))
            continue
        regiao_codigo = info.loc[cod_ibge, "regiao_codigo"]
        regiao_nome = info.loc[cod_ibge, "regiao_nome"]

        for ano in anos:
            try:
                meses = _coletar_um_municipio_ano(cod_ibge, ano)
            except Exception as e:
                logger.error(f"  {cod_ibge} {ano}: erro — {e}")
                pbar.update(1)
                continue

            for entrada in meses:
                linhas.append({
                    "cod_ibge": cod_ibge,
                    "regiao_codigo": regiao_codigo,
                    "regiao_nome": regiao_nome,
                    **entrada,
                    "total": entrada["icms"] + entrada["ipva"],
                })
            time.sleep(throttle)
            pbar.update(1)

    pbar.close()

    if not linhas:
        logger.warning("SICONFI Cota-Parte: nenhum dado coletado")
        return pd.DataFrame()

    df = pd.DataFrame(linhas).sort_values(
        ["ano", "mes", "regiao_codigo", "cod_ibge"]
    ).reset_index(drop=True)

    save_dataframe(
        df, "transf_estaduais_ce_mensal", subdir="processed", path_parts=["sefaz_ce_siconfi"],
    )
    logger.info(
        f"SICONFI Cota-Parte: {len(df)} linhas, {df['cod_ibge'].nunique()} munic., "
        f"R$ {df['total'].sum() / 1e9:.2f} bi total"
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

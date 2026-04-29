"""
Coletor de Formação Bruta de Capital Fixo (FBCF) do Brasil — IpeaData.

A série mensal do "Indicador IPEA de FBCF" (`GAC12_INDFBCF12`) é um índice de
volume com base 1995=100. Para obter R$ a preços constantes, mensalizamos o
total anual (`SCN10_FBKFP10`, FBCF a preços de 2010) usando a proporção do
índice mensal dentro de cada ano.

Aplicação no painel regional CE: assume-se que o investimento total do CE
corresponde ao share do PIB CE/Brasil aplicado ao FBCF Brasil mensal — é uma
hipótese explícita do Prof. Paulo, conforme áudio de abr/2026:

    invest_total_CE_mensal = FBCF_BR_mensal × share_PIB_CE_BR (~2,2%)

E o investimento privado é calculado por residual:

    invest_privado_CE = invest_total_CE - invest_estadual - invest_municipal - invest_federal

## Fonte

- Mensal (índice): IpeaData série ``GAC12_INDFBCF12`` — base 1995=100
- Mensal dessazonalizado: ``GAC12_INDFBCFDESSAZ12``
- Anual (R$ 2010 reais): IpeaData série ``SCN10_FBKFP10`` (Sistema de Contas
  Nacionais — IBGE, Tabela 1846)
- API: ``http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='...')``

Saída: ``dados_nordeste/processed/ipea/fbcf_brasil_mensal.csv`` com colunas
(ano, mes, indice, indice_dessaz, fbcf_R$_2010_milhoes).
"""

from __future__ import annotations

import logging

import pandas as pd
import requests

from pipeline.config import REQUEST_TIMEOUT
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

API_BASE = "http://www.ipeadata.gov.br/api/odata4/ValoresSerie"

SERIES = {
    "indice": "GAC12_INDFBCF12",
    "indice_dessaz": "GAC12_INDFBCFDESSAZ12",
    "fbcf_anual_real_2010": "SCN10_FBKFP10",  # R$ milhões a preços de 2010
}


def _baixar_serie(sercodigo: str) -> pd.DataFrame:
    """Baixa todos os valores de uma série IpeaData via OData."""
    url = f"{API_BASE}(SERCODIGO='{sercodigo}')"
    logger.info(f"  IpeaData: baixando {sercodigo}")
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json().get("value", [])
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    # IpeaData manda timezone offset por linha — converter cada um e remover tz
    df["data"] = pd.to_datetime(df["VALDATA"], utc=True).dt.tz_localize(None)
    df["valor"] = pd.to_numeric(df["VALVALOR"], errors="coerce")
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    return df[["ano", "mes", "valor"]]


def coletar_fbcf_brasil() -> pd.DataFrame:
    """
    Coleta FBCF Brasil mensal e converte para R$ a preços de 2010.

    Estratégia: para cada ano A, distribui o total anual ``SCN10_FBKFP10[A]``
    entre os 12 meses na proporção do índice mensal ``GAC12_INDFBCF12``.
    Anos sem dado anual (típico: ano corrente) usam o total do ano anterior
    como proxy, escalonado pela razão de índices anuais.
    """
    indice = _baixar_serie(SERIES["indice"]).rename(columns={"valor": "indice"})
    indice_ds = _baixar_serie(SERIES["indice_dessaz"]).rename(
        columns={"valor": "indice_dessaz"}
    )
    fbcf_anual = _baixar_serie(SERIES["fbcf_anual_real_2010"]).rename(
        columns={"valor": "fbcf_anual"}
    )
    fbcf_anual = fbcf_anual.groupby("ano", as_index=False)["fbcf_anual"].sum()

    if indice.empty or fbcf_anual.empty:
        logger.error("IpeaData: alguma série retornou vazia.")
        return pd.DataFrame()

    df = indice.merge(indice_ds, on=["ano", "mes"], how="left")

    # Calcular fator R$/índice por ano: fator = total_R$_anual / soma_indice_anual
    soma_idx_anual = df.groupby("ano", as_index=False)["indice"].sum().rename(
        columns={"indice": "soma_indice_anual"}
    )
    fatores = soma_idx_anual.merge(fbcf_anual, on="ano", how="left")
    fatores["fator_r$"] = fatores["fbcf_anual"] / fatores["soma_indice_anual"]

    # Para anos sem fbcf_anual, usar o último fator disponível
    fatores = fatores.sort_values("ano")
    fatores["fator_r$"] = fatores["fator_r$"].ffill()

    df = df.merge(fatores[["ano", "fator_r$"]], on="ano", how="left")
    df["fbcf_r$_2010_milhoes"] = df["indice"] * df["fator_r$"]
    df = df.drop(columns=["fator_r$"]).sort_values(["ano", "mes"]).reset_index(drop=True)

    save_dataframe(
        df,
        "fbcf_brasil_mensal",
        subdir="processed",
        path_parts=["ipea"],
    )
    logger.info(
        f"FBCF Brasil mensal: {len(df)} obs ({df['ano'].min()}-{df['ano'].max()}), "
        f"último mês: {df.iloc[-1]['ano']:.0f}/{df.iloc[-1]['mes']:02.0f}"
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_fbcf_brasil()

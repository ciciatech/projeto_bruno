"""
Coletor de Bolsa Família / Auxílio Brasil / Novo Bolsa Família por município CE.

Cobre a transição entre os três programas:
- 2015-01 a 2021-10: Bolsa Família (clássico)
- 2021-11 a 2023-02: Auxílio Brasil
- 2023-03 em diante: Novo Bolsa Família

Consome os CSVs bulk públicos do Portal da Transparência (sem necessidade de
API key). Cada mês é um ZIP com ~100 MB / CSV ~2 GB descomprimido — o módulo
``_portal_transp_csv`` cuida de filtrar UF=CE em chunks para não estourar memória.

Saída: ``dados_nordeste/processed/bolsa_familia/bf_municipio_ce_mensal.csv``
com colunas (cod_ibge, regiao_codigo, regiao_nome, ano, mes, valor_total, beneficiarios).
"""

from __future__ import annotations

import logging

import pandas as pd

from pipeline.config import PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL
from pipeline.extract._portal_transp_csv import coletar_programa_mensal
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)


# Marcos da transição entre programas. Ano-mês inclusive.
BOLSA_FAMILIA_FIM = (2021, 10)
AUXILIO_BRASIL_INICIO = (2021, 11)
AUXILIO_BRASIL_FIM = (2023, 2)
NOVO_BF_INICIO = (2023, 3)

URL_PATH_POR_PROGRAMA = {
    "bolsa_familia_classico": "bolsa-familia-pagamentos",
    "auxilio_brasil": "auxilio-brasil",
    "novo_bolsa_familia": "novo-bolsa-familia",
}


def _periodo_para_programa(programa: str, ano_ini: int, ano_fim: int) -> tuple[int, int, int, int]:
    """Calcula intervalo (ano_ini, mes_ini, ano_fim, mes_fim) válido para o programa."""
    if programa == "bolsa_familia_classico":
        ini_a, ini_m = ano_ini, 1
        fim_a, fim_m = min(ano_fim, BOLSA_FAMILIA_FIM[0]), (
            BOLSA_FAMILIA_FIM[1] if ano_fim >= BOLSA_FAMILIA_FIM[0] else 12
        )
    elif programa == "auxilio_brasil":
        ini_a, ini_m = AUXILIO_BRASIL_INICIO
        if ano_ini > ini_a:
            ini_a, ini_m = ano_ini, 1
        fim_a, fim_m = min(ano_fim, AUXILIO_BRASIL_FIM[0]), (
            AUXILIO_BRASIL_FIM[1] if ano_fim >= AUXILIO_BRASIL_FIM[0] else 12
        )
    elif programa == "novo_bolsa_familia":
        ini_a, ini_m = NOVO_BF_INICIO
        if ano_ini > ini_a:
            ini_a, ini_m = ano_ini, 1
        fim_a, fim_m = ano_fim, 12
    else:
        raise ValueError(f"Programa desconhecido: {programa}")
    return ini_a, ini_m, fim_a, fim_m


def coletar_ce(
    ano_inicio: int = PERIODO_INICIO_MENSAL,
    ano_fim: int = PERIODO_FIM_MENSAL,
    manter_zip: bool = False,
    pular_meses_existentes: bool = True,
) -> pd.DataFrame:
    """
    Coleta Bolsa Família/Auxílio Brasil/Novo BF mensal por município CE
    para todo o período. Faz a junção dos três programas em um único painel.
    """
    frames = []
    for programa, url_path in URL_PATH_POR_PROGRAMA.items():
        ini_a, ini_m, fim_a, fim_m = _periodo_para_programa(programa, ano_inicio, ano_fim)
        if (ini_a, ini_m) > (fim_a, fim_m):
            continue
        logger.info(f"=== {programa}: {ini_a:04d}/{ini_m:02d} a {fim_a:04d}/{fim_m:02d} ===")
        df = coletar_programa_mensal(
            nome_programa=f"bolsa_familia/{programa}",
            url_path=url_path,
            ano_inicio=ini_a,
            ano_fim=fim_a,
            mes_inicio=ini_m,
            mes_fim_ano_corrente=fim_m,
            manter_zip=manter_zip,
            pular_meses_existentes=pular_meses_existentes,
        )
        if not df.empty:
            df["programa"] = programa
            frames.append(df)

    if not frames:
        logger.warning("Bolsa Família: nenhum dado coletado.")
        return pd.DataFrame()

    consolidado = pd.concat(frames, ignore_index=True).sort_values(
        ["ano", "mes", "cod_ibge"]
    ).reset_index(drop=True)

    save_dataframe(
        consolidado,
        "bf_municipio_ce_mensal",
        subdir="processed",
        path_parts=["bolsa_familia"],
    )
    logger.info(
        f"Bolsa Família consolidado: {len(consolidado)} linhas, "
        f"{consolidado['cod_ibge'].nunique()} municípios, "
        f"{consolidado.groupby(['ano','mes']).ngroups} meses."
    )
    return consolidado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    # Por default, coletar 2015-2025. Pode sobrescrever via importar e chamar.
    coletar_ce()

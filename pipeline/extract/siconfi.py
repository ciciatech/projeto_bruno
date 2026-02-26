"""
Extração de dados fiscais do SICONFI (Tesouro Nacional).

Endpoints: RREO, RGF, DCA.
"""

import time
import logging

import pandas as pd
from tqdm import tqdm

from pipeline.config import ESTADOS_NE, PERIODO_INICIO, PERIODO_FIM
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)


class Siconfi:
    """
    Coleta dados fiscais do SICONFI (Tesouro Nacional).

    API pública: https://apidatalake.tesouro.gov.br/ords/siconfi/tt/
    """

    BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

    @staticmethod
    def coletar_rreo(ano: int, periodo: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """
        Coleta RREO (Relatório Resumido de Execução Orçamentária).

        periodo: 1 a 6 (bimestral)
        """
        url = f"{Siconfi.BASE_URL}/rreo"
        params = {
            "an_exercicio": ano,
            "nr_periodo": periodo,
            "co_tipo_demonstrativo": "RREO",
            "id_ente": cod_ibge,
        }
        logger.info(f"SICONFI RREO: {uf} {ano} período {periodo}...")
        resp = safe_request(url, params=params, timeout=90)
        if resp is None:
            return pd.DataFrame()

        data = resp.json()
        items = data.get("items", [])
        if not items:
            logger.warning(f"SICONFI RREO vazio: {uf} {ano} P{periodo}")
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["uf"] = uf
        return df

    @staticmethod
    def coletar_rgf(ano: int, periodo: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """
        Coleta RGF (Relatório de Gestão Fiscal).

        periodo: 1 a 3 (quadrimestral)
        """
        url = f"{Siconfi.BASE_URL}/rgf"
        params = {
            "an_exercicio": ano,
            "nr_periodo": periodo,
            "in_periodicidade": "Q",
            "co_tipo_demonstrativo": "RGF",
            "id_ente": cod_ibge,
            "co_poder": "E",
        }
        logger.info(f"SICONFI RGF: {uf} {ano} período {periodo}...")
        resp = safe_request(url, params=params, timeout=90)
        if resp is None:
            return pd.DataFrame()

        data = resp.json()
        items = data.get("items", [])
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["uf"] = uf
        return df

    @staticmethod
    def coletar_dca(ano: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """
        Coleta DCA (Declaração de Contas Anuais).
        """
        url = f"{Siconfi.BASE_URL}/dca"
        params = {
            "an_exercicio": ano,
            "id_ente": cod_ibge,
        }
        logger.info(f"SICONFI DCA: {uf} {ano}...")
        resp = safe_request(url, params=params, timeout=90)
        if resp is None:
            return pd.DataFrame()

        data = resp.json()
        items = data.get("items", [])
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["uf"] = uf
        return df

    @classmethod
    def coletar_rreo_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta RREO de todos os estados do NE para todos os anos."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="SICONFI RREO - Anos"):
            for uf, info in ESTADOS_NE.items():
                for periodo in range(1, 7):  # 6 bimestres
                    df = cls.coletar_rreo(ano, periodo, info["cod_ibge"], uf)
                    if not df.empty:
                        frames.append(df)
                    time.sleep(1)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siconfi_rreo_nordeste")
        return df_all

    @classmethod
    def coletar_rgf_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta RGF de todos os estados do NE."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="SICONFI RGF - Anos"):
            for uf, info in ESTADOS_NE.items():
                for periodo in range(1, 4):  # 3 quadrimestres
                    df = cls.coletar_rgf(ano, periodo, info["cod_ibge"], uf)
                    if not df.empty:
                        frames.append(df)
                    time.sleep(1)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siconfi_rgf_nordeste")
        return df_all

    @classmethod
    def coletar_dca_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta DCA de todos os estados do NE."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="SICONFI DCA - Anos"):
            for uf, info in ESTADOS_NE.items():
                df = cls.coletar_dca(ano, info["cod_ibge"], uf)
                if not df.empty:
                    frames.append(df)
                time.sleep(1)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siconfi_dca_nordeste")
        return df_all

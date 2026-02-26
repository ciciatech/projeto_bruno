"""
Extração de transferências constitucionais (FPE, FPM, FUNDEB) via RREO/SICONFI.
"""

import time
import logging

import pandas as pd
from tqdm import tqdm

from pipeline.config import ESTADOS_NE, PERIODO_INICIO, PERIODO_FIM
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)


class TransferenciasConstitucionais:
    """
    Extrai dados de transferências constitucionais do RREO (SICONFI).

    O RREO (Anexo 01 - Balanço Orçamentário) contém as receitas de
    transferências correntes e de capital recebidas pelos estados,
    incluindo FPE, FPM, FUNDEB, SUS, FNDE, etc.
    """

    BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"

    ANEXOS_TRANSFERENCIAS = [
        "RREO-Anexo 01",
        "RREO-Anexo 06",
    ]

    TERMOS_TRANSFERENCIA = [
        "transfer",
        "fpe",
        "fpm",
        "fundeb",
        "fundef",
        "cota-parte",
        "cide",
        "royalties",
        "compensações financeiras",
    ]

    @classmethod
    def coletar_transferencias_rreo(
        cls, ano: int, cod_ibge: str, uf: str
    ) -> pd.DataFrame:
        """Coleta dados de transferências via RREO (Anexos 01 e 06)."""
        frames = []
        for anexo in cls.ANEXOS_TRANSFERENCIAS:
            for periodo in range(1, 7):
                url = cls.BASE_URL
                params = {
                    "an_exercicio": ano,
                    "nr_periodo": periodo,
                    "co_tipo_demonstrativo": "RREO",
                    "no_anexo": anexo,
                    "id_ente": cod_ibge,
                }
                logger.info(f"Transferências ({anexo}): {uf} {ano} P{periodo}...")
                resp = safe_request(url, params=params, timeout=90)
                if resp is None:
                    continue

                data = resp.json()
                items = data.get("items", [])
                if not items:
                    continue

                df = pd.DataFrame(items)
                termos = "|".join(cls.TERMOS_TRANSFERENCIA)
                mask = df["conta"].str.lower().str.contains(termos, na=False)
                df_transf = df[mask].copy()
                if not df_transf.empty:
                    df_transf["uf"] = uf
                    frames.append(df_transf)
                time.sleep(1)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @classmethod
    def coletar_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta transferências de todos os estados do NE via RREO."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="Transferências Constitucionais"):
            for uf, info in ESTADOS_NE.items():
                df = cls.coletar_transferencias_rreo(ano, info["cod_ibge"], uf)
                if not df.empty:
                    frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "transferencias_constitucionais_nordeste")
        return df_all

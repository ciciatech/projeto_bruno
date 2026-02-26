"""
Extração de séries temporais do Banco Central via API SGS.
"""

import time
import logging

import pandas as pd

from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)


class BacenSGS:
    """Coleta de séries temporais do Banco Central via API SGS."""

    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"

    # Séries relevantes para a tese
    SERIES = {
        # IBCR Nordeste (com ajuste sazonal)
        25389: "IBCR_NE_ajuste_sazonal",
        # Saldo de crédito PF - Nordeste
        14084: "credito_PF_nordeste",
        # Saldo de crédito PJ - Nordeste
        14089: "credito_PJ_nordeste",
        # Saldo total de crédito - Nordeste
        14079: "credito_total_nordeste",
        # SELIC acumulada no mês (anualizada) - série mensal
        4189: "selic_mensal",
        # IPCA mensal
        433: "ipca_mensal",
        # PIB trimestral (índice de volume)
        22109: "pib_trimestral_indice",
        # Taxa de inadimplência PF
        21084: "inadimplencia_PF",
        # Taxa de inadimplência PJ
        21085: "inadimplencia_PJ",
        # Saldo crédito PF - Brasil (para comparação)
        20539: "credito_PF_brasil",
        # Saldo crédito PJ - Brasil (para comparação)
        20541: "credito_PJ_brasil",
    }

    @staticmethod
    def coletar_serie(
        codigo_serie: int,
        nome: str,
        data_inicio: str = "01/01/2015",
        data_fim: str = "31/12/2025",
    ) -> pd.DataFrame:
        """Coleta uma série do SGS/BACEN."""
        url = BacenSGS.BASE_URL.format(serie=codigo_serie)
        params = {
            "dataInicial": data_inicio,
            "dataFinal": data_fim,
        }
        logger.info(f"BACEN-SGS: Coletando série {codigo_serie} ({nome})...")
        resp = safe_request(url, params=params)
        if resp is None:
            logger.error(f"Falha ao coletar série {codigo_serie}")
            return pd.DataFrame()

        try:
            data = resp.json()
        except Exception:
            logger.error(f"Resposta não-JSON para série {codigo_serie}")
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning(f"Série {codigo_serie} retornou vazia.")
            return df

        df.rename(columns={"data": "data", "valor": "valor"}, inplace=True)
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df["serie_codigo"] = codigo_serie
        df["serie_nome"] = nome
        return df

    @classmethod
    def coletar_todas(cls) -> pd.DataFrame:
        """Coleta todas as séries configuradas."""
        frames = []
        for codigo, nome in cls.SERIES.items():
            df = cls.coletar_serie(codigo, nome)
            if not df.empty:
                frames.append(df)
            time.sleep(1)  # rate limit

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "bacen_sgs_series")

        # Pivot para formato wide (útil para análise)
        df_wide = df_all.pivot_table(
            index="data", columns="serie_nome", values="valor"
        ).reset_index()
        save_dataframe(df_wide, "bacen_sgs_wide")

        return df_all

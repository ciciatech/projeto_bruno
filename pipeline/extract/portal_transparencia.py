"""
Extração de transferências federais via Portal da Transparência.

API: http://api.portaldatransparencia.gov.br/
"""

import os
import time
import logging

import pandas as pd

from pipeline.config import ESTADOS_NE, PERIODO_FIM, PERIODO_INICIO
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)


class PortalTransparencia:
    """
    Coleta de transferências federais via Portal da Transparência.

    NOTA: A API requer cadastro e chave (API Key) gratuita.
    Cadastre-se em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
    """

    BASE_URL = "http://api.portaldatransparencia.gov.br/api-de-dados"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("PORTAL_TRANSPARENCIA_API_KEY")
        self.headers = {}
        if self.api_key:
            self.headers["chave-api-dados"] = self.api_key
            logger.info("Portal da Transparência: API Key configurada.")
        else:
            logger.warning(
                "Portal da Transparência: Sem API Key. "
                "Cadastre-se em https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email "
                "e configure via env var PORTAL_TRANSPARENCIA_API_KEY ou parâmetro api_key."
            )

    def coletar_bolsa_familia_por_estado(
        self, ano: int, mes: int, uf: str
    ) -> pd.DataFrame:
        """
        Coleta dados do Bolsa Família / Auxílio Brasil por UF via API.
        Endpoint: /bolsa-familia-por-municipio
        """
        if not self.api_key:
            logger.warning("API Key necessária para esta consulta.")
            return pd.DataFrame()

        url = f"{self.BASE_URL}/bolsa-familia-por-municipio"
        params = {
            "mesAno": f"{ano:04d}{mes:02d}",
            "codigoIbge": ESTADOS_NE[uf]["cod_ibge"],
            "pagina": 1,
        }
        all_records = []
        while True:
            resp = safe_request(url, params=params, headers=self.headers)
            if resp is None or not resp.json():
                break
            records = resp.json()
            all_records.extend(records)
            if len(records) < 15:  # página padrão
                break
            params["pagina"] += 1
            time.sleep(0.5)

        if not all_records:
            return pd.DataFrame()

        df = pd.json_normalize(all_records, sep="_")
        df["uf"] = uf
        df["ano"] = ano
        df["mes"] = mes
        return df

    @staticmethod
    def _agregar_bolsa_familia_por_uf(df: pd.DataFrame) -> pd.DataFrame:
        """Agrega registros municipais do Bolsa Família para UF x mês."""
        if df.empty:
            return df

        out = df.copy()
        sum_cols = []
        for col in out.columns:
            col_lower = col.lower()
            if any(token in col_lower for token in ["valor", "benefici", "quant"]):
                out[col] = pd.to_numeric(out[col], errors="coerce")
                if out[col].notna().any():
                    sum_cols.append(col)

        grouped = out.groupby(["ano", "mes", "uf"], as_index=False).size()
        grouped.rename(columns={"size": "registros_municipais"}, inplace=True)

        if sum_cols:
            sums = out.groupby(["ano", "mes", "uf"], as_index=False)[sum_cols].sum(min_count=1)
            grouped = grouped.merge(sums, on=["ano", "mes", "uf"], how="left")

        grouped["data"] = pd.to_datetime(
            grouped["ano"].astype(str) + "-" + grouped["mes"].astype(str).str.zfill(2) + "-01"
        )
        grouped.sort_values(["data", "uf"], inplace=True)
        grouped.reset_index(drop=True, inplace=True)
        return grouped

    def coletar_bolsa_familia_nordeste(
        self,
        ano_inicio: int = PERIODO_INICIO,
        ano_fim: int = PERIODO_FIM,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Coleta Bolsa Família mensal por UF para todo o Nordeste."""
        if not self.api_key:
            logger.warning("Bolsa Família via Portal da Transparência requer API Key.")
            return pd.DataFrame(), pd.DataFrame()

        frames = []
        for ano in range(ano_inicio, ano_fim + 1):
            for mes in range(1, 13):
                for uf in ESTADOS_NE:
                    df = self.coletar_bolsa_familia_por_estado(ano, mes, uf)
                    if not df.empty:
                        frames.append(df)
                    time.sleep(0.15)

        if not frames:
            return pd.DataFrame(), pd.DataFrame()

        raw_df = pd.concat(frames, ignore_index=True)
        agg_df = self._agregar_bolsa_familia_por_uf(raw_df)

        save_dataframe(
            raw_df,
            "bolsa_familia_portal_transparencia",
            path_parts=["bolsa_familia", "nordeste"],
        )
        save_dataframe(
            agg_df,
            "bolsa_familia_uf_mensal",
            path_parts=["bolsa_familia", "nordeste"],
        )
        return raw_df, agg_df

    def coletar_transferencias_por_uf(
        self, ano: int, mes: int, uf: str
    ) -> pd.DataFrame:
        """
        Coleta transferências federais (todas as modalidades) por UF.
        Endpoint: /transferencias/por-unidade-federativa
        """
        if not self.api_key:
            return pd.DataFrame()

        url = f"{self.BASE_URL}/transferencias"
        params = {
            "mesAno": f"{ano:04d}{mes:02d}",
            "codigoUF": ESTADOS_NE[uf]["cod_ibge"],
            "pagina": 1,
        }
        all_records = []
        while True:
            resp = safe_request(url, params=params, headers=self.headers)
            if resp is None:
                break
            data = resp.json()
            if not data:
                break
            all_records.extend(data)
            if len(data) < 15:
                break
            params["pagina"] += 1
            time.sleep(0.5)

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        df["uf"] = uf
        return df

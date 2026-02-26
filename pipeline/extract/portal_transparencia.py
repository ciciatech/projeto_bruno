"""
Extração de transferências federais via Portal da Transparência.

API: http://api.portaldatransparencia.gov.br/
"""

import os
import time
import logging

import pandas as pd

from pipeline.config import ESTADOS_NE
from pipeline.utils import safe_request

logger = logging.getLogger(__name__)


class PortalTransparencia:
    """
    Coleta de transferências federais via Portal da Transparência.

    NOTA: A API requer cadastro e chave (API Key) gratuita.
    Cadastre-se em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
    """

    BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"

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

        df = pd.DataFrame(all_records)
        df["uf"] = uf
        df["ano"] = ano
        df["mes"] = mes
        return df

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

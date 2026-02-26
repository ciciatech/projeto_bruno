"""
Extração de dados do Bolsa Família / Auxílio Brasil / Novo Bolsa Família.

Fontes:
- https://dados.gov.br/ (datasets de transferência de renda)
- https://aplicacoes.mds.gov.br/sagi/vis/data3/ (Vis Data)
"""

import logging

import pandas as pd

from pipeline.config import PERIODO_INICIO, PERIODO_FIM
from pipeline.utils import safe_request

logger = logging.getLogger(__name__)


class BolsaFamilia:
    """
    Coleta dados do Bolsa Família / Auxílio Brasil / Novo Bolsa Família
    via Portal de Dados Abertos e Vis Data MDS.
    """

    # API do SAGI/MDS para dados do Bolsa Família por município
    SAGI_URL = "https://aplicacoes.mds.gov.br/sagi/servicos/misocial"

    @staticmethod
    def coletar_via_api_sagi(ano: int, mes: int, cod_ibge_uf: str) -> pd.DataFrame:
        """
        Tenta coletar dados do MDS/SAGI.
        NOTA: Esta API pode ter restrições ou estar indisponível.
        Alternativa: download manual dos CSVs em dados.gov.br
        """
        url = f"{BolsaFamilia.SAGI_URL}"
        params = {
            "ano": ano,
            "mes": mes,
            "codigo_ibge": cod_ibge_uf,
            "tipo": "1",  # Bolsa Família
        }
        resp = safe_request(url, params=params, timeout=30)
        if resp is None:
            return pd.DataFrame()

        try:
            data = resp.json()
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def gerar_urls_download_dados_abertos() -> list[dict]:
        """
        Gera URLs para download dos datasets do Bolsa Família no dados.gov.br.

        NOTA: As URLs mudam conforme o programa vigente:
        - 2015-2021: Bolsa Família
        - 2021-2023: Auxílio Brasil
        - 2023+: Novo Bolsa Família
        """
        urls = []
        base_msg = (
            "Datasets disponíveis em dados.gov.br:\n"
            "  - Bolsa Família (2015-2021): https://dados.gov.br/dados/conjuntos-dados/bolsa-familia-pagamentos\n"
            "  - Auxílio Brasil (2021-2023): https://dados.gov.br/dados/conjuntos-dados/auxilio-brasil\n"
            "  - Novo Bolsa Família (2023+): https://dados.gov.br/dados/conjuntos-dados/bolsa-familia-pagamentos\n"
        )
        logger.info(base_msg)

        for ano in range(PERIODO_INICIO, PERIODO_FIM + 1):
            for mes in range(1, 13):
                urls.append(
                    {
                        "ano": ano,
                        "mes": mes,
                        "url": f"https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/{ano:04d}{mes:02d}",
                        "descricao": f"Bolsa Família {ano:04d}/{mes:02d}",
                    }
                )
        return urls

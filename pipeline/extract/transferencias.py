"""
Extração de transferências constitucionais (FPE, FPM, FUNDEB) via RREO/SICONFI.
"""

import time
import logging

import pandas as pd
try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - fallback para ambientes mínimos
    def tqdm(iterable, **kwargs):
        return iterable

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

    ANEXOS_TRANSFERENCIAS = ["RREO-Anexo 01", "RREO-Anexo 06"]

    # Lista auditável de rubricas contábeis relevantes para a tese.
    # Prioriza transferências intergovernamentais, legais e fundos,
    # evitando depender apenas de busca textual ampla.
    CONTAS_PERMITIDAS = {
        "CompensacoesFinanceiras": "compensacoes_financeiras",
        "TransferenciasCorrentes": "transferencias_correntes",
        "TransferenciasCorrentesIntergovernamentais": "transferencias_correntes_intergovernamentais",
        "TransferenciasCorrentesDaUniaoEDeSuasEntidades": "transferencias_correntes_uniao",
        "TransferenciasCorrentesDosEstadosEDoDistritoFederalEDeSuasEntidades": "transferencias_correntes_estados",
        "TransferenciasCorrentesDosMunicipiosEDeSuasEntidades": "transferencias_correntes_municipios",
        "TransferenciasDeCapital": "transferencias_capital",
        "TransferenciasDeCapitalIntergovernamentais": "transferencias_capital_intergovernamentais",
        "TransferenciasDeCapitalDaUniaoEDeSuasEntidades": "transferencias_capital_uniao",
        "TransferenciasDeCapitalDosEstadosEDoDistritoFederalEDeSuasEntidades": "transferencias_capital_estados",
        "TransferenciasdeCapitalDosMunicipiosEDeSuasEntidades": "transferencias_capital_municipios",
        "TransferenciasDoFUNDEB": "fundeb",
        "TransferenciasDaLC871996": "lc_87_1996",
        "TransferenciasDaLCn611989": "lc_61_1989",
        "TransferenciasConstitucionaisELegaisLC156": "transferencias_constitucionais_legais",
        "RREO6CotaParteDoFPE": "fpe",
        "RREO6TransferenciasConstitucionaisELegais": "transferencias_constitucionais_legais",
        "RREO6TransferenciasCorrentes": "transferencias_correntes",
        "RREO6TransferenciasDeCapital": "transferencias_capital",
        "RREO6OutrasTransferenciasCorrentes": "outras_transferencias_correntes",
        "RREO6OutrasTransferenciasDeCapital": "outras_transferencias_capital",
    }

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
                mask = df["cod_conta"].isin(cls.CONTAS_PERMITIDAS)
                df_transf = df[mask].copy()
                if not df_transf.empty:
                    df_transf["uf"] = uf
                    df_transf["categoria_transferencia"] = df_transf["cod_conta"].map(
                        cls.CONTAS_PERMITIDAS
                    )
                    df_transf["criterio_coleta"] = "whitelist_cod_conta"
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
        save_dataframe(
            df_all,
            "transferencias_constitucionais_nordeste",
            path_parts=["transferencias", "nordeste"],
        )
        return df_all

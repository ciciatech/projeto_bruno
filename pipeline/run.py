"""
Orquestrador principal do pipeline de coleta de dados.

## Modos de uso

### Pipeline NE legado (UF × bimestre)

  python3 -m pipeline.run --apenas-bacen
  python3 -m pipeline.run --modulos bacen siconfi_rreo
  python3 -m pipeline.run --full

### Pipeline regional CE (município → 14 regiões, mensal)

  python3 -m pipeline.run --apenas-ce            # só coletas CE
  python3 -m pipeline.run --modulos-ce estban bolsa_familia_ce bpc
  python3 -m pipeline.run --full-ce              # coletas CE + painel regional

Os módulos CE específicos disponíveis:
  - estban             : ESTBAN/BNB municipal (precisa arquivos manuais)
  - bolsa_familia_ce   : BF/Auxílio Brasil/Novo BF municipal via Portal Transp
  - bpc                : BPC municipal via Portal Transp
  - transferencias_municipais : FPM/FUNDEB/ITR via CKAN STN
  - sefaz_ce           : ICMS/IPVA cota-parte (precisa arquivos manuais)
  - invest_municipal   : adapta planilha do Bruno
  - invest_federal     : transferências voluntárias federais via Portal Transp
  - caged_municipal    : CAGED por município (pesado, requer FTP MTE)
  - bacen              : recoleta BACEN incluindo IBCR-CE
"""

import json
import logging
from datetime import datetime

from pipeline.config import (
    BASE_DIR,
    PERIODO_INICIO,
    PERIODO_FIM,
    ESTADOS_NE,
    RAW_DIR,
    PROCESSED_DIR,
    LOGS_DIR,
)
from pipeline.utils import setup_logging

# Coletores legados (NE × UF × bimestre)
from pipeline.extract.bacen import BacenSGS
from pipeline.extract.siconfi import Siconfi
from pipeline.extract.portal_transparencia import PortalTransparencia
from pipeline.extract import bolsa_familia as bolsa_familia_ce_mod  # reescrito (CE municipal)
from pipeline.extract.transferencias import TransferenciasConstitucionais
from pipeline.extract.caged_rais import CagedRais
from pipeline.quality import executar_auditoria_qualidade
from pipeline.transform.etl import executar_etl
from pipeline.transform.preparacao_modelo import executar_preparacao

# Coletores novos (CE × município × mês)
from pipeline.extract import (
    estban as estban_mod,
    bpc as bpc_mod,
    transferencias_municipais as transf_mun_mod,
    sefaz_ce as sefaz_ce_mod,
    invest_municipal_planilha as invest_mun_mod,
    invest_federal as invest_fed_mod,
    caged_municipal as caged_mun_mod,
)
from pipeline.transform.preparacao_modelo_regional import construir_painel as construir_painel_ce

logger = logging.getLogger(__name__)


MODULOS_NE = [
    "bacen",
    "siconfi_rreo",
    "siconfi_rgf",
    "siconfi_dca",
    "transferencias",
    "caged_rais",
    "auditoria_qualidade",
]

MODULOS_CE = [
    "bacen",  # inclui IBCR-CE
    "estban",
    "bolsa_familia_ce",
    "bpc",
    "transferencias_municipais",
    "sefaz_ce",
    "invest_municipal",
    "invest_federal",
    "caged_municipal",
]


class PipelineColeta:
    """Orquestrador da pipeline de coleta de dados."""

    def __init__(self, api_key_portal: str = None):
        self.portal = PortalTransparencia(api_key=api_key_portal)
        self.resumo = {}

    def executar(self, modulos: list[str] = None):
        """Executa coleta dos módulos do pipeline NE legado."""
        if modulos is None:
            modulos = [
                "bacen",
                "siconfi_rreo",
                "siconfi_rgf",
                "siconfi_dca",
                "transferencias",
            ]

        logger.info("=" * 70)
        logger.info("PIPELINE DE COLETA DE DADOS - TESE DESP/UFC")
        logger.info(f"Módulos: {modulos}")
        logger.info(f"Período: {PERIODO_INICIO}-{PERIODO_FIM}")
        logger.info(f"Diretório: {BASE_DIR.absolute()}")
        logger.info("=" * 70)

        inicio = datetime.now()

        if "bacen" in modulos:
            logger.info("\n>>> MÓDULO: BACEN-SGS <<<")
            df = BacenSGS.coletar_todas()
            self.resumo["bacen"] = len(df)

        if "siconfi_rreo" in modulos:
            logger.info("\n>>> MÓDULO: SICONFI - RREO <<<")
            df = Siconfi.coletar_rreo_nordeste()
            self.resumo["siconfi_rreo"] = len(df)

        if "siconfi_rgf" in modulos:
            logger.info("\n>>> MÓDULO: SICONFI - RGF <<<")
            df = Siconfi.coletar_rgf_nordeste()
            self.resumo["siconfi_rgf"] = len(df)

        if "siconfi_dca" in modulos:
            logger.info("\n>>> MÓDULO: SICONFI - DCA <<<")
            df = Siconfi.coletar_dca_nordeste()
            self.resumo["siconfi_dca"] = len(df)

        if "transferencias" in modulos:
            logger.info("\n>>> MÓDULO: Transferências Constitucionais (RREO) <<<")
            df = TransferenciasConstitucionais.coletar_nordeste()
            self.resumo["transferencias"] = len(df)

        if "caged_rais" in modulos:
            logger.info("\n>>> MÓDULO: CAGED / RAIS legado (NE × UF) <<<")
            total = CagedRais.coletar_todas()
            self.resumo["caged_rais"] = total

        if "auditoria_qualidade" in modulos:
            logger.info("\n>>> MÓDULO: Auditoria de Qualidade <<<")
            report = executar_auditoria_qualidade()
            self.resumo["auditoria_qualidade_fontes"] = report["summary"]["total_sources"]
            self.resumo["auditoria_qualidade_alertas"] = report["summary"]["alerta"]
            self.resumo["auditoria_qualidade_erros"] = report["summary"]["erro"]

        self._encerrar(inicio, modulos)
        return self.resumo

    def executar_ce(self, modulos: list[str] = None):
        """Executa coleta dos módulos do pipeline regional CE."""
        if modulos is None:
            modulos = MODULOS_CE

        logger.info("=" * 70)
        logger.info("PIPELINE REGIONAL CEARÁ - 14 regiões × meses")
        logger.info(f"Módulos: {modulos}")
        logger.info("=" * 70)

        inicio = datetime.now()

        if "bacen" in modulos:
            logger.info("\n>>> CE: BACEN-SGS (inclui IBCR-CE série 25380) <<<")
            df = BacenSGS.coletar_todas()
            self.resumo["bacen"] = len(df)

        if "estban" in modulos:
            logger.info("\n>>> CE: ESTBAN BNB municipal <<<")
            df = estban_mod.coletar_de_diretorio()
            self.resumo["estban"] = len(df)

        if "bolsa_familia_ce" in modulos:
            logger.info("\n>>> CE: Bolsa Família / Aux. Brasil / Novo BF municipal <<<")
            df = bolsa_familia_ce_mod.coletar_ce()
            self.resumo["bolsa_familia_ce"] = len(df)

        if "bpc" in modulos:
            logger.info("\n>>> CE: BPC municipal <<<")
            df = bpc_mod.coletar_ce()
            self.resumo["bpc"] = len(df)

        if "transferencias_municipais" in modulos:
            logger.info("\n>>> CE: Transferências constitucionais STN municipais <<<")
            df = transf_mun_mod.coletar_ce()
            self.resumo["transferencias_municipais"] = len(df)

        if "sefaz_ce" in modulos:
            logger.info("\n>>> CE: Transferências estaduais SEFAZ-CE (adapter) <<<")
            df = sefaz_ce_mod.coletar_de_diretorio()
            self.resumo["sefaz_ce"] = len(df)

        if "invest_municipal" in modulos:
            logger.info("\n>>> CE: Investimento municipal (planilha Bruno) <<<")
            df = invest_mun_mod.coletar_de_diretorio()
            self.resumo["invest_municipal"] = len(df)

        if "invest_federal" in modulos:
            logger.info("\n>>> CE: Investimento federal (transf. voluntárias) <<<")
            df = invest_fed_mod.coletar_ce()
            self.resumo["invest_federal"] = len(df)

        if "caged_municipal" in modulos:
            logger.info("\n>>> CE: CAGED municipal (FTP MTE — pesado) <<<")
            df = caged_mun_mod.coletar_ce()
            self.resumo["caged_municipal"] = len(df)

        self._encerrar(inicio, modulos, escopo="CE")
        return self.resumo

    def _encerrar(self, inicio: datetime, modulos: list[str], escopo: str = "NE"):
        """Imprime resumo e salva metadados."""
        fim = datetime.now()
        duracao = fim - inicio
        logger.info("\n" + "=" * 70)
        logger.info(f"RESUMO DA COLETA ({escopo})")
        logger.info("=" * 70)
        for modulo, qtd in self.resumo.items():
            logger.info(f"  {modulo}: {qtd} registros")
        logger.info(f"  Duração total: {duracao}")
        logger.info("=" * 70)

        meta = {
            "data_execucao": fim.isoformat(),
            "duracao_segundos": duracao.total_seconds(),
            "escopo": escopo,
            "periodo": f"{PERIODO_INICIO}-{PERIODO_FIM}",
            "estados": list(ESTADOS_NE.keys()) if escopo == "NE" else ["CE"],
            "modulos_executados": modulos,
            "resumo": self.resumo,
        }
        meta_path = BASE_DIR / "metadata_coleta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        logger.info(f"Metadados: {meta_path}")


def executar_fluxo_completo(api_key: str = None, modulos: list[str] = None) -> dict:
    """Coleta NE legada + ETL + preparação modelo (UF/bimestre) + auditoria."""
    pipeline = PipelineColeta(api_key_portal=api_key)
    resumo = pipeline.executar(modulos=modulos)
    logger.info("\n>>> ETAPA EXTRA: ETL <<<")
    executar_etl()
    logger.info("\n>>> ETAPA EXTRA: PREPARAÇÃO DO MODELO (UF/bimestre) <<<")
    executar_preparacao()
    logger.info("\n>>> ETAPA EXTRA: AUDITORIA DE QUALIDADE <<<")
    report = executar_auditoria_qualidade()
    resumo["painel_tese_fontes_auditadas"] = report["summary"]["total_sources"]
    resumo["painel_tese_alertas"] = report["summary"]["alerta"]
    resumo["painel_tese_erros"] = report["summary"]["erro"]
    return resumo


def executar_fluxo_completo_ce(modulos: list[str] = None) -> dict:
    """Coleta CE municipal + construção do painel regional."""
    pipeline = PipelineColeta()
    resumo = pipeline.executar_ce(modulos=modulos)
    logger.info("\n>>> ETAPA: PAINEL REGIONAL CE (14 regiões × meses) <<<")
    painel = construir_painel_ce()
    resumo["painel_regional_linhas"] = len(painel)
    resumo["painel_regional_colunas"] = len(painel.columns)
    return resumo


if __name__ == "__main__":
    import argparse

    for d in [RAW_DIR, PROCESSED_DIR, LOGS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    setup_logging()

    parser = argparse.ArgumentParser(
        description="Pipeline de coleta de dados - Tese DESP/UFC (Bruno Cardoso)"
    )

    # Pipeline legado NE
    parser.add_argument("--modulos", nargs="+", default=None, choices=MODULOS_NE,
                        help="Módulos do pipeline NE legado a executar")
    parser.add_argument("--apenas-bacen", action="store_true",
                        help="Executa apenas BACEN-SGS (NE)")
    parser.add_argument("--etl", action="store_true", help="Apenas ETL (raw → processed)")
    parser.add_argument("--preparar-modelo", action="store_true",
                        help="Apenas preparação modelo NE/UF/bimestre")
    parser.add_argument("--auditar", action="store_true", help="Apenas auditoria de qualidade")
    parser.add_argument("--full", action="store_true",
                        help="NE: coleta + ETL + preparação + auditoria")

    # Pipeline regional CE (novo)
    parser.add_argument("--modulos-ce", nargs="+", default=None, choices=MODULOS_CE,
                        help="Módulos do pipeline regional CE a executar")
    parser.add_argument("--apenas-ce", action="store_true",
                        help="Executa todos os coletores do pipeline regional CE")
    parser.add_argument("--full-ce", action="store_true",
                        help="CE: coletas + construção do painel regional 14×meses")
    parser.add_argument("--painel-ce", action="store_true",
                        help="Apenas reconstrói o painel regional CE a partir dos consolidados existentes")

    parser.add_argument("--api-key", type=str, default=None,
                        help="API Key do Portal da Transparência (opcional)")

    args = parser.parse_args()

    # Despachar — pipeline CE tem precedência se especificado
    if args.painel_ce:
        construir_painel_ce()
    elif args.full_ce:
        executar_fluxo_completo_ce(modulos=args.modulos_ce)
    elif args.apenas_ce or args.modulos_ce:
        pipeline = PipelineColeta(api_key_portal=args.api_key)
        pipeline.executar_ce(modulos=args.modulos_ce)
    elif args.full:
        modulos = ["bacen"] if args.apenas_bacen else args.modulos
        executar_fluxo_completo(api_key=args.api_key, modulos=modulos)
    elif args.etl:
        executar_etl()
    elif args.preparar_modelo:
        executar_preparacao()
    elif args.auditar:
        executar_auditoria_qualidade()
    else:
        modulos = ["bacen"] if args.apenas_bacen else args.modulos
        pipeline = PipelineColeta(api_key_portal=args.api_key)
        pipeline.executar(modulos=modulos)

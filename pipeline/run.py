"""
Orquestrador principal do pipeline de coleta de dados.

Uso:
  python3 -m pipeline.run
  python3 -m pipeline.run --apenas-bacen
  python3 -m pipeline.run --modulos bacen siconfi_rreo
  python3 -m pipeline.run --full
"""

import json
import logging
from datetime import datetime

import pandas as pd

from pipeline.config import BASE_DIR, PERIODO_INICIO, PERIODO_FIM, ESTADOS_NE, RAW_DIR, PROCESSED_DIR, LOGS_DIR
from pipeline.utils import setup_logging, save_dataframe
from pipeline.extract.bacen import BacenSGS
from pipeline.extract.siconfi import Siconfi
from pipeline.extract.portal_transparencia import PortalTransparencia
from pipeline.extract.bolsa_familia import BolsaFamilia
from pipeline.extract.transferencias import TransferenciasConstitucionais
from pipeline.extract.caged_rais import CagedRais
from pipeline.quality import executar_auditoria_qualidade
from pipeline.transform.etl import executar_etl
from pipeline.transform.preparacao_modelo import executar_preparacao

logger = logging.getLogger(__name__)


class PipelineColeta:
    """Orquestrador da pipeline de coleta de dados."""

    def __init__(self, api_key_portal: str = None):
        self.portal = PortalTransparencia(api_key=api_key_portal)
        self.resumo = {}

    def executar(self, modulos: list[str] = None):
        """
        Executa a coleta completa ou parcial.

        modulos: lista de módulos a executar. Se None, executa todos.
        Opções: ["bacen", "siconfi_rreo", "siconfi_rgf", "siconfi_dca",
                 "transferencias", "bolsa_familia", "caged_rais",
                 "auditoria_qualidade"]
        """
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

        # --- BACEN SGS ---
        if "bacen" in modulos:
            logger.info("\n>>> MÓDULO 1: BACEN-SGS <<<")
            df = BacenSGS.coletar_todas()
            self.resumo["bacen"] = len(df)

        # --- SICONFI RREO ---
        if "siconfi_rreo" in modulos:
            logger.info("\n>>> MÓDULO 2: SICONFI - RREO <<<")
            df = Siconfi.coletar_rreo_nordeste()
            self.resumo["siconfi_rreo"] = len(df)

        # --- SICONFI RGF ---
        if "siconfi_rgf" in modulos:
            logger.info("\n>>> MÓDULO 3: SICONFI - RGF <<<")
            df = Siconfi.coletar_rgf_nordeste()
            self.resumo["siconfi_rgf"] = len(df)

        # --- SICONFI DCA ---
        if "siconfi_dca" in modulos:
            logger.info("\n>>> MÓDULO 4: SICONFI - DCA <<<")
            df = Siconfi.coletar_dca_nordeste()
            self.resumo["siconfi_dca"] = len(df)

        # --- Transferências Constitucionais ---
        if "transferencias" in modulos:
            logger.info("\n>>> MÓDULO 5: Transferências Constitucionais <<<")
            df = TransferenciasConstitucionais.coletar_nordeste()
            self.resumo["transferencias"] = len(df)

        # --- Bolsa Família (gera URLs para download) ---
        if "bolsa_familia" in modulos:
            logger.info("\n>>> MÓDULO 6: Bolsa Família / Auxílio Brasil <<<")
            resumo_bf = BolsaFamilia.coletar_nordeste(self.portal)
            self.resumo["bolsa_familia_raw"] = resumo_bf["registros_raw"]
            self.resumo["bolsa_familia_uf_mensal"] = resumo_bf["registros_uf_mensal"]
            self.resumo["bolsa_familia_urls"] = resumo_bf["urls_download"]

        # --- CAGED / RAIS (FTP MTE/PDET) ---
        if "caged_rais" in modulos:
            logger.info("\n>>> MÓDULO 7: CAGED / RAIS (FTP MTE/PDET) <<<")
            total = CagedRais.coletar_todas()
            self.resumo["caged_rais"] = total

        # --- Auditoria de qualidade dos dados ---
        if "auditoria_qualidade" in modulos:
            logger.info("\n>>> MÓDULO 8: Auditoria de Qualidade <<<")
            report = executar_auditoria_qualidade()
            self.resumo["auditoria_qualidade_fontes"] = report["summary"]["total_sources"]
            self.resumo["auditoria_qualidade_alertas"] = report["summary"]["alerta"]
            self.resumo["auditoria_qualidade_erros"] = report["summary"]["erro"]

        # --- Resumo Final ---
        fim = datetime.now()
        duracao = fim - inicio
        logger.info("\n" + "=" * 70)
        logger.info("RESUMO DA COLETA")
        logger.info("=" * 70)
        for modulo, qtd in self.resumo.items():
            logger.info(f"  {modulo}: {qtd} registros")
        logger.info(f"  Duração total: {duracao}")
        logger.info(f"  Dados salvos em: {BASE_DIR.absolute()}")
        logger.info("=" * 70)

        # Salva metadados da execução
        meta = {
            "data_execucao": fim.isoformat(),
            "duracao_segundos": duracao.total_seconds(),
            "periodo": f"{PERIODO_INICIO}-{PERIODO_FIM}",
            "estados": list(ESTADOS_NE.keys()),
            "modulos_executados": modulos,
            "resumo": self.resumo,
        }
        meta_path = BASE_DIR / "metadata_coleta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        logger.info(f"Metadados: {meta_path}")

        return self.resumo


def executar_fluxo_completo(api_key: str = None, modulos: list[str] = None) -> dict:
    """
    Executa a esteira principal:
      1. coleta
      2. etl
      3. preparação do modelo
      4. auditoria de qualidade
    """
    pipeline = PipelineColeta(api_key_portal=api_key)
    resumo = pipeline.executar(modulos=modulos)
    logger.info("\n>>> ETAPA EXTRA: ETL <<<")
    executar_etl()
    logger.info("\n>>> ETAPA EXTRA: PREPARAÇÃO DO MODELO <<<")
    executar_preparacao()
    logger.info("\n>>> ETAPA EXTRA: AUDITORIA DE QUALIDADE <<<")
    report = executar_auditoria_qualidade()
    resumo["painel_tese_fontes_auditadas"] = report["summary"]["total_sources"]
    resumo["painel_tese_alertas"] = report["summary"]["alerta"]
    resumo["painel_tese_erros"] = report["summary"]["erro"]
    return resumo


if __name__ == "__main__":
    import argparse

    # Garante que os diretórios existam
    for d in [RAW_DIR, PROCESSED_DIR, LOGS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    setup_logging()

    parser = argparse.ArgumentParser(
        description="Pipeline de coleta de dados públicos - Tese DESP/UFC"
    )
    parser.add_argument(
        "--modulos",
        nargs="+",
        default=None,
        choices=[
            "bacen",
            "siconfi_rreo",
            "siconfi_rgf",
            "siconfi_dca",
            "transferencias",
            "bolsa_familia",
            "caged_rais",
            "auditoria_qualidade",
        ],
        help="Módulos a executar (default: todos exceto bolsa_familia, caged_rais e auditoria_qualidade)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="API Key do Portal da Transparência",
    )
    parser.add_argument(
        "--apenas-bacen",
        action="store_true",
        help="Executa apenas a coleta do BACEN-SGS (rápido, para teste)",
    )
    parser.add_argument(
        "--etl",
        action="store_true",
        help="Executa apenas a etapa de ETL (raw -> processed).",
    )
    parser.add_argument(
        "--preparar-modelo",
        action="store_true",
        help="Executa apenas a preparação do modelo (deflacionamento + painel final).",
    )
    parser.add_argument(
        "--auditar",
        action="store_true",
        help="Executa apenas a auditoria de qualidade.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Executa coleta + ETL + preparação do modelo + auditoria.",
    )

    args = parser.parse_args()

    if args.full:
        modulos = ["bacen"] if args.apenas_bacen else args.modulos
        executar_fluxo_completo(api_key=args.api_key, modulos=modulos)
    elif args.etl:
        executar_etl()
    elif args.preparar_modelo:
        executar_preparacao()
    elif args.auditar:
        executar_auditoria_qualidade()
    else:
        if args.apenas_bacen:
            modulos = ["bacen"]
        else:
            modulos = args.modulos
        pipeline = PipelineColeta(api_key_portal=args.api_key)
        pipeline.executar(modulos=modulos)

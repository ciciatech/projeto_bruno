"""
Orquestrador principal do pipeline de coleta de dados.

Uso:
  python -m pipeline.run
  python -m pipeline.run --apenas-bacen
  python -m pipeline.run --modulos bacen siconfi_rreo
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
                 "transferencias", "bolsa_familia"]
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
            urls = BolsaFamilia.gerar_urls_download_dados_abertos()
            urls_df = pd.DataFrame(urls)
            save_dataframe(urls_df, "bolsa_familia_urls_download")
            self.resumo["bolsa_familia_urls"] = len(urls)

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
        ],
        help="Módulos a executar (default: todos exceto bolsa_familia)",
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

    args = parser.parse_args()

    if args.apenas_bacen:
        modulos = ["bacen"]
    else:
        modulos = args.modulos

    pipeline = PipelineColeta(api_key_portal=args.api_key)
    pipeline.executar(modulos=modulos)

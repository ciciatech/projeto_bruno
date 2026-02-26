"""
Utilitários compartilhados do pipeline.

Funções de request HTTP, persistência de DataFrames e logging.
"""

import time
import logging
from datetime import datetime
from typing import Optional

import requests
import pandas as pd

from pipeline.config import (
    RAW_DIR,
    PROCESSED_DIR,
    LOGS_DIR,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY,
)

logger = logging.getLogger(__name__)


def setup_logging():
    """Configura logging com arquivo rotativo e saída no console."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(
                LOGS_DIR / f"coleta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            ),
            logging.StreamHandler(),
        ],
    )


def safe_request(
    url: str,
    params: dict = None,
    headers: dict = None,
    timeout: int = REQUEST_TIMEOUT,
    retries: int = MAX_RETRIES,
) -> Optional[requests.Response]:
    """Requisição HTTP com retry e tratamento de erro."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            logger.warning(
                f"HTTP {resp.status_code} em {url} (tentativa {attempt}/{retries}): {e}"
            )
            if resp.status_code == 429:
                wait = RETRY_DELAY * attempt * 2
                logger.info(f"Rate limit. Aguardando {wait}s...")
                time.sleep(wait)
            elif resp.status_code >= 500:
                time.sleep(RETRY_DELAY * attempt)
            else:
                return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro em {url} (tentativa {attempt}/{retries}): {e}")
            time.sleep(RETRY_DELAY * attempt)
    logger.error(f"Falha definitiva após {retries} tentativas: {url}")
    return None


def save_dataframe(df: pd.DataFrame, filename: str, subdir: str = "raw"):
    """Salva DataFrame como CSV e Parquet."""
    target_dir = RAW_DIR if subdir == "raw" else PROCESSED_DIR
    csv_path = target_dir / f"{filename}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Salvo: {csv_path} ({len(df)} registros)")
    try:
        parquet_path = target_dir / f"{filename}.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Salvo: {parquet_path}")
    except Exception:
        logger.warning(f"Parquet não disponível para {filename}. Apenas CSV salvo.")

"""
Utilitários compartilhados do pipeline.

Funções de request HTTP, persistência de DataFrames e logging.
"""

import logging
import time
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


def save_dataframe(
    df: pd.DataFrame,
    filename: str,
    subdir: str = "raw",
    path_parts: list[str] | tuple[str, ...] | None = None,
):
    """Salva DataFrame apenas como CSV, com suporte a subpastas."""
    target_root = RAW_DIR if subdir == "raw" else PROCESSED_DIR
    target_dir = target_root
    if path_parts:
        target_dir = target_root.joinpath(*path_parts)
    target_dir.mkdir(parents=True, exist_ok=True)

    csv_path = target_dir / f"{filename}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Salvo: {csv_path} ({len(df)} registros)")


def meses_faltantes_interior(df: pd.DataFrame) -> list[str]:
    """Meses sem NENHUMA linha entre o primeiro e o último mês presentes.

    Para fontes de publicação mensal contínua (CAGED, STN), um buraco no MEIO
    da série nunca é ausência real de dado — é falha de download/parse engolida
    por um WARNING (incidente de 2026-06-10: FTP do MTE devolveu 550 transitório
    em 202503, o pipeline seguiu e o painel saiu com o mês faltando codificado
    como sucesso). Bordas (antes do primeiro/depois do último mês publicado)
    não contam: refletem a cobertura real da fonte.

    Espera colunas ``ano`` e ``mes``; retorna chaves ``'AAAAMM'`` ordenadas,
    vazia quando a série é contínua.
    """
    if df is None or df.empty or not {"ano", "mes"}.issubset(df.columns):
        return []
    presentes = {
        (int(a), int(m))
        for a, m in df[["ano", "mes"]].drop_duplicates().itertuples(index=False)
    }
    (ano_i, mes_i), (ano_f, mes_f) = min(presentes), max(presentes)
    faltantes = []
    ano, mes = ano_i, mes_i
    while (ano, mes) <= (ano_f, mes_f):
        if (ano, mes) not in presentes:
            faltantes.append(f"{ano:04d}{mes:02d}")
        mes += 1
        if mes == 13:
            ano, mes = ano + 1, 1
    return faltantes

"""
Extrator de dados de execução orçamentária — Portal da Transparência de Alagoas.

Fonte: https://www.transparencia.al.gov.br
API REST JSON pública, sem autenticação.
Licença: Creative Commons BY-SA 4.0.
"""

import logging
import re

import pandas as pd

from pipeline.config import PERIODO_INICIO, PERIODO_FIM, RAW_DIR
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)

BASE_URL = "http://transparencia.al.gov.br"
ENDPOINT = "/orcamento/json-execucao-orcamentaria-avancada-filtro/"
PAGE_SIZE = 1000

# Colunas de visualização e valor solicitadas na API
VISUALIZAR = [
    "ano",
    "ug",
    "descricao_ug",
    "pt_funcao_id__descricao_funcao",
]
VALORES = [
    "total_inicial",
    "total_atualizado",
    "total_empenhado",
    "total_liquidado",
    "total_pago",
]

# Mapa de renomeação para nomes amigáveis
RENAME_COLS = {
    "ano": "ano",
    "ug": "cod_ug",
    "descricao_ug": "unidade_gestora",
    "pt_funcao_id__descricao_funcao": "funcao",
    "valor_total_inicial": "dotacao_inicial",
    "valor_total_atualizado": "dotacao_atualizada",
    "valor_total_empenhado": "empenhado",
    "valor_total_liquidado": "liquidado",
    "valor_total_pago": "pago",
}


def _parse_br_number(value: str) -> float:
    """Converte número no formato brasileiro (1.234.567,89) para float."""
    if not value or not isinstance(value, str):
        return 0.0
    cleaned = value.strip()
    cleaned = re.sub(r"[^\d,\-]", "", cleaned)
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


class TransparenciaAL:
    """Extrator da API de execução orçamentária do Portal Transparência AL."""

    @staticmethod
    def coletar_ano(ano: int) -> pd.DataFrame | None:
        """Coleta execução orçamentária de um ano com paginação."""
        cache_paths = [
            RAW_DIR / "execucao_orcamentaria" / "al" / f"transparencia_al_{ano}.csv",
            RAW_DIR / "transparencia" / "al" / f"transparencia_al_{ano}.csv",
        ]
        if cache_paths[0].exists() or cache_paths[1].exists():
            logger.info(f"AL {ano}: cache encontrado, pulando.")
            for cache_path in cache_paths:
                if cache_path.exists():
                    return pd.read_csv(cache_path)

        logger.info(f"AL {ano}: coletando dados...")
        all_rows = []
        offset = 0

        while True:
            params = [
                ("limit", PAGE_SIZE),
                ("offset", offset),
                ("ano__in", ano),
            ]
            for v in VISUALIZAR:
                params.append(("visualizar", v))
            for v in VALORES:
                params.append(("valor", v))

            resp = safe_request(f"{BASE_URL}{ENDPOINT}", params=params)
            if resp is None:
                logger.error(f"AL {ano}: falha na requisição (offset={offset})")
                break

            data = resp.json()
            rows = data.get("rows", [])
            total = data.get("total", 0)

            if not rows:
                break

            all_rows.extend(rows)
            offset += PAGE_SIZE
            logger.info(f"AL {ano}: {len(all_rows)}/{total} registros")

            if offset >= total:
                break

        if not all_rows:
            logger.warning(f"AL {ano}: nenhum dado retornado.")
            return None

        df = pd.DataFrame(all_rows)

        # Converter colunas de valor (formato BR -> float)
        valor_cols = [c for c in df.columns if c.startswith("valor_")]
        for col in valor_cols:
            df[col] = df[col].apply(_parse_br_number)

        # Garantir ano como int
        if "ano" in df.columns:
            df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

        # Renomear colunas
        df.rename(columns=RENAME_COLS, inplace=True)

        save_dataframe(
            df,
            f"transparencia_al_{ano}",
            path_parts=["execucao_orcamentaria", "al"],
        )
        logger.info(f"AL {ano}: {len(df)} registros salvos.")
        return df

    @classmethod
    def coletar_todas(cls, inicio: int = PERIODO_INICIO, fim: int = PERIODO_FIM) -> pd.DataFrame | None:
        """Coleta todos os anos e consolida em um único DataFrame."""
        RAW_DIR.mkdir(parents=True, exist_ok=True)

        frames = []
        for ano in range(inicio, fim + 1):
            df = cls.coletar_ano(ano)
            if df is not None:
                frames.append(df)

        if not frames:
            logger.error("AL: nenhum dado coletado.")
            return None

        consolidado = pd.concat(frames, ignore_index=True)
        save_dataframe(
            consolidado,
            "transparencia_al_consolidado",
            path_parts=["execucao_orcamentaria", "al"],
        )
        logger.info(f"AL consolidado: {len(consolidado)} registros totais.")
        return consolidado

"""
Extrator de dados de execução orçamentária — Portal da Transparência do Piauí.

Fonte: https://transparencia2.pi.gov.br
API REST JSON pública, sem autenticação, mas requer User-Agent.
"""

import logging
import pandas as pd
from datetime import datetime

from pipeline.config import PERIODO_INICIO, PERIODO_FIM, RAW_DIR
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)

BASE_URL = "https://transparencia2.pi.gov.br"
ENDPOINT = "/api/v2/despesas/{ano}/01/12/"
PAGE_SIZE = 1000

# Mapa de renomeação para nomes amigáveis (seguindo padrão do painel)
RENAME_COLS = {
    "exercicio": "ano",
    "unidade_gestora_codigo": "cod_ug",
    "unidade_gestora_titulo": "unidade_gestora",
    "funcao_titulo": "funcao",
    "valor_empenhado": "empenhado",
    "valor_pago": "pago",
    "valor_liquidado": "liquidado",
    "emissao_data": "data",
    "credor_titulo": "favorecido"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


class TransparenciaPI:
    """Extrator da API de execução orçamentária do Portal Transparência PI."""

    @staticmethod
    def coletar_ano(ano: int) -> pd.DataFrame | None:
        """Coleta execução orçamentária de um ano com paginação."""
        cache_paths = [
            RAW_DIR / "execucao_orcamentaria" / "pi" / f"transparencia_pi_{ano}.csv",
            RAW_DIR / "transparencia" / "pi" / f"transparencia_pi_{ano}.csv",
        ]
        if cache_paths[0].exists() or cache_paths[1].exists():
            logger.info(f"PI {ano}: cache encontrado, pulando.")
            for cache_path in cache_paths:
                if cache_path.exists():
                    return pd.read_csv(cache_path)

        logger.info(f"PI {ano}: coletando dados...")
        all_rows = []
        page = 1

        url = f"{BASE_URL}{ENDPOINT.format(ano=ano)}"

        while True:
            params = {
                "limit": PAGE_SIZE,
                "page": page
            }

            resp = safe_request(url, headers=HEADERS, params=params)
            if resp is None:
                logger.error(f"PI {ano}: falha na requisição (page={page})")
                break

            try:
                data = resp.json()
            except ValueError:
                logger.error(f"PI {ano}: resposta não é JSON válido (page={page})")
                break
                
            rows = data.get("results", [])
            total = data.get("count", 0)

            if not rows:
                break

            all_rows.extend(rows)
            logger.info(f"PI {ano}: {len(all_rows)}/{total} registros (Página {page})")

            # Verifica se há próxima página
            if not data.get("next"):
                break
                
            page += 1

        if not all_rows:
            logger.warning(f"PI {ano}: nenhum dado retornado.")
            return None

        df = pd.DataFrame(all_rows)

        # Tratar colunas numéricas de valor
        valor_cols = ["valor_empenhado", "valor_liquidado", "valor_pago"]
        for col in valor_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Garantir ano como int
        if "exercicio" in df.columns:
            df["exercicio"] = pd.to_numeric(df["exercicio"], errors="coerce").astype("Int64")

        # Renomear colunas
        df.rename(columns=RENAME_COLS, inplace=True)

        # Adicionar coluna 'dotacao_atualizada' calculando o máximo entre empenhado, liquidado e pago 
        # (se PI não fornece o valor da dotação na despesa)
        if "empenhado" in df.columns:
            # Em Piauí, a chamada do endpoint detalhado pode não ter dotação no nível de empenho.
            # Vamos estimar ou pelo menos colocar o empenhado como proxy limite se nulo.
            df["dotacao_atualizada"] = df[["empenhado", "liquidado", "pago"]].max(axis=1)
            df["dotacao_inicial"] = 0.0

        save_dataframe(
            df,
            f"transparencia_pi_{ano}",
            path_parts=["execucao_orcamentaria", "pi"],
        )
        logger.info(f"PI {ano}: {len(df)} registros salvos.")
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
            logger.error("PI: nenhum dado coletado.")
            return None

        consolidado = pd.concat(frames, ignore_index=True)
        save_dataframe(
            consolidado,
            "transparencia_pi_consolidado",
            path_parts=["execucao_orcamentaria", "pi"],
        )
        logger.info(f"PI consolidado: {len(consolidado)} registros totais.")
        return consolidado

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TransparenciaPI.coletar_ano(datetime.now().year)

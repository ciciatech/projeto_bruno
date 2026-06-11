"""
Ingestão da aba "Instrumentos estaduais" da planilha do Prof. Paulo.

Fonte
-----
Planilha entregue pelo orientador (Prof. Paulo) em 10/06/2026:
``docs/dados_prof_paulo/Dados Regionais - CC SEFAZ e Tese Bruno-2.xlsx``,
aba ``Instrumentos estaduais``. Série bimestral do Ceará com rótulos
``15b1``..``25b6`` (2015–2025, 66 observações).

Conceitos e unidades
--------------------
- ``resultado_primario_prev_real_dez25`` — Resultado Primário **incluindo o
  resultado previdenciário** (conceito adotado pelo orientador; **difere do
  RREO**, em que o resultado primário é apurado sem o componente
  previdenciário). Valores **já deflacionados para R$ de dez/2025** — não
  deflacionar novamente.
- ``dcl_real_dez25`` — Dívida Consolidada Líquida do estado, também **já em
  R$ de dez/2025**.
- ``selic`` — taxa SELIC em fração decimal (ex.: 0.1415 = 14,15% a.a.).
- ``ibcr_ce`` — IBCR-CE (Índice de Atividade Econômica Regional do BACEN).

Esta planilha é a série **canônica** de RP+Previdenciário e DCL do CE no
projeto. Já SELIC e IBCR-CE são redundantes com ``pipeline/extract/bacen.py``
(séries SGS 4189 e 25380): a **fonte canônica dessas duas continua sendo o
SGS/BACEN** — elas ficam neste CSV apenas como verificação cruzada contra a
planilha do orientador.

Saída
-----
``dados_nordeste/processed/instrumentos_estaduais/instrumentos_estaduais_ce_bimestral.csv``
com colunas ``(ano, bimestre, resultado_primario_prev_real_dez25,
dcl_real_dez25, selic, ibcr_ce)``.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

# Raiz do repo (pipeline/extract/ -> pipeline/ -> raiz); independe do cwd.
REPO_ROOT = Path(__file__).resolve().parents[2]
PLANILHA_PATH = (
    REPO_ROOT / "docs" / "dados_prof_paulo"
    / "Dados Regionais - CC SEFAZ e Tese Bruno-2.xlsx"
)
ABA = "Instrumentos estaduais"

# Headers exatos da aba (linha 2 da planilha) -> nome canônico no CSV
COLUNAS_ABA = {
    "Resultado Primário + Previdenciário": "resultado_primario_prev_real_dez25",
    "DCL": "dcl_real_dez25",
    "SELIC": "selic",
    "IBCR-CE": "ibcr_ce",
}
COL_PERIODO = "PERÍODO"

# Rótulo de período: "15b1".."25b6" (AAbN -> ano 20AA, bimestre N)
RE_PERIODO = re.compile(r"^\s*(\d{2})b([1-6])\s*$")


def _parse_periodo(rotulo) -> tuple[int, int] | None:
    """Converte rótulo '15b1' -> (2015, 1). Retorna None se não for período."""
    if not isinstance(rotulo, str):
        return None
    m = RE_PERIODO.match(rotulo)
    if not m:
        return None
    return 2000 + int(m.group(1)), int(m.group(2))


def coletar(planilha: Path | str | None = None) -> pd.DataFrame:
    """Lê a aba "Instrumentos estaduais" e salva o CSV bimestral canônico.

    Linhas sem rótulo de período válido (título, observações de rodapé) são
    descartadas. Retorna o DataFrame salvo, ordenado por (ano, bimestre).
    """
    planilha = Path(planilha) if planilha else PLANILHA_PATH
    if not planilha.exists():
        raise FileNotFoundError(f"Planilha do orientador não encontrada: {planilha}")

    bruto = pd.read_excel(planilha, sheet_name=ABA, header=1)
    faltantes = [c for c in [COL_PERIODO, *COLUNAS_ABA] if c not in bruto.columns]
    if faltantes:
        raise ValueError(
            f"Aba '{ABA}' sem as colunas esperadas {faltantes}. "
            f"Headers encontrados: {list(bruto.columns)}"
        )

    periodos = bruto[COL_PERIODO].map(_parse_periodo)
    sel = bruto.loc[periodos.notna()]
    if sel.empty:
        raise ValueError(f"Aba '{ABA}' sem nenhuma linha com período 'AAbN'.")

    df = pd.DataFrame(
        {
            "ano": [p[0] for p in periodos.loc[sel.index]],
            "bimestre": [p[1] for p in periodos.loc[sel.index]],
        }
    )
    for header, canonico in COLUNAS_ABA.items():
        df[canonico] = pd.to_numeric(sel[header], errors="coerce").to_numpy()

    df = df.sort_values(["ano", "bimestre"]).reset_index(drop=True)

    save_dataframe(
        df,
        "instrumentos_estaduais_ce_bimestral",
        subdir="processed",
        path_parts=["instrumentos_estaduais"],
    )
    logger.info(
        f"Instrumentos estaduais: {len(df)} bimestres "
        f"({df['ano'].min()}b{df.loc[0, 'bimestre']}–"
        f"{df['ano'].max()}b{df.loc[len(df) - 1, 'bimestre']})"
    )
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar()

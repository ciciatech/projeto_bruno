"""Testes das séries SGS da letter Matos & Araújo (T31) — sem rede.

Cobrem (1) o registro das 8 séries no dict ``BacenSGS.SERIES`` e (2) o schema
do CSV wide já coletado (presença das 8 colunas, cobertura 2011–2025 e
spot-check de plausibilidade da inadimplência do crédito livre às famílias).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.extract.bacen import BacenSGS

WIDE_CSV = Path("dados_nordeste/raw/bacen/nacional/bacen_sgs_wide.csv")

SERIES_LETTER = {
    29027: "renda_disponivel_familias_letter",
    22110: "crescimento_consumo_familias_letter",
    20570: "estoque_credito_livre_familias_letter",
    20606: "estoque_credito_direcionado_familias_letter",
    21112: "inadimplencia_credito_livre_familias_letter",
    21145: "inadimplencia_credito_direcionado_familias_letter",
    25462: "juros_credito_livre_familias_letter",
    25493: "juros_credito_direcionado_familias_letter",
}


def test_series_letter_registradas_no_coletor():
    """As 8 séries da letter precisam estar no dict de séries do coletor."""
    for codigo, nome in SERIES_LETTER.items():
        assert codigo in BacenSGS.SERIES, f"Série {codigo} ausente de BacenSGS.SERIES"
        assert BacenSGS.SERIES[codigo] == nome
        assert nome.endswith("_letter"), f"{nome} sem sufixo _letter"


def test_data_inicio_default_cobre_janela_da_letter():
    """Default de coleta deve começar em 2011 (letter usa 2011T2–2025T4)."""
    default_inicio = BacenSGS.coletar_serie.__defaults__[0]
    assert default_inicio == "01/01/2011"


@pytest.mark.skipif(
    not WIDE_CSV.exists(),
    reason="bacen_sgs_wide.csv ainda não coletado (rode BacenSGS.coletar_todas())",
)
def test_bacen_sgs_wide_schema_letter():
    """CSV wide: 8 colunas _letter presentes, cobertura ≥ 2011–2025."""
    df = pd.read_csv(WIDE_CSV, parse_dates=["data"])

    faltantes = set(SERIES_LETTER.values()) - set(df.columns)
    assert not faltantes, f"Colunas _letter ausentes no wide: {faltantes}"

    for nome in SERIES_LETTER.values():
        serie = df.set_index("data")[nome].dropna()
        assert not serie.empty, f"{nome} sem observações"
        # Letter usa 2011T2+; as séries de inadimplência/juros nascem em 03/2011
        assert serie.index.min().year == 2011, f"{nome} não cobre 2011"
        assert serie.index.max().year >= 2025, f"{nome} não cobre 2025"


@pytest.mark.skipif(
    not WIDE_CSV.exists(),
    reason="bacen_sgs_wide.csv ainda não coletado (rode BacenSGS.coletar_todas())",
)
def test_inadimplencia_livre_familias_plausivel():
    """Spot-check: média da série 21112 deve ficar entre 4% e 8%."""
    df = pd.read_csv(WIDE_CSV, parse_dates=["data"])
    media = df["inadimplencia_credito_livre_familias_letter"].mean()
    assert 4.0 <= media <= 8.0, f"Média implausível para 21112: {media:.2f}"

"""Testes da ingestão da aba "Instrumentos estaduais" (planilha do orientador).

Sem rede: a planilha está versionada no repo em ``docs/dados_prof_paulo/``.
Cobrem o shape da série (66 bimestres, 15b1–25b6), o sinal da DCL em 25b6 e
spot-check de 2 valores contra a aba lida na hora via openpyxl.
"""

from __future__ import annotations

import openpyxl
import pandas as pd
import pytest

from pipeline.extract import instrumentos_estaduais_planilha as mod

COLUNAS_CSV = [
    "ano",
    "bimestre",
    "resultado_primario_prev_real_dez25",
    "dcl_real_dez25",
    "selic",
    "ibcr_ce",
]


@pytest.fixture
def df(tmp_path, monkeypatch):
    """Roda o coletor redirecionando o PROCESSED_DIR para tmp_path."""
    monkeypatch.setattr("pipeline.utils.PROCESSED_DIR", tmp_path)
    return mod.coletar()


def test_shape_66_bimestres_15b1_a_25b6(df, tmp_path):
    """66 linhas, cobertura completa 2015b1–2025b6, sem duplicatas."""
    assert list(df.columns) == COLUNAS_CSV
    assert len(df) == 66

    esperado = {(ano, bim) for ano in range(2015, 2026) for bim in range(1, 7)}
    assert set(zip(df["ano"], df["bimestre"])) == esperado
    assert not df.duplicated(subset=["ano", "bimestre"]).any()

    # Ordenado: primeira linha 15b1, última 25b6
    assert (df.iloc[0]["ano"], df.iloc[0]["bimestre"]) == (2015, 1)
    assert (df.iloc[-1]["ano"], df.iloc[-1]["bimestre"]) == (2025, 6)

    # CSV salvo no destino canônico (redirecionado para tmp_path)
    csv = tmp_path / "instrumentos_estaduais" / "instrumentos_estaduais_ce_bimestral.csv"
    assert csv.exists()
    salvo = pd.read_csv(csv, encoding="utf-8-sig")
    assert list(salvo.columns) == COLUNAS_CSV
    assert len(salvo) == 66


def test_25b6_dcl_positiva(df):
    """Último bimestre da série (25b6) tem DCL > 0."""
    ult = df[(df["ano"] == 2025) & (df["bimestre"] == 6)]
    assert len(ult) == 1
    assert ult["dcl_real_dez25"].iloc[0] > 0


def _valor_aba(rotulo: str, header: str) -> float:
    """Lê na hora, via openpyxl, o valor da aba para (período, coluna)."""
    wb = openpyxl.load_workbook(mod.PLANILHA_PATH, read_only=True, data_only=True)
    try:
        ws = wb[mod.ABA]
        linhas = list(ws.iter_rows(values_only=True))
    finally:
        wb.close()
    headers = list(linhas[1])  # linha 2 da aba = cabeçalho
    col = headers.index(header)
    for linha in linhas[2:]:
        if isinstance(linha[0], str) and linha[0].strip() == rotulo:
            return float(linha[col])
    raise AssertionError(f"Período {rotulo} não encontrado na aba '{mod.ABA}'")


def test_spot_check_contra_aba_lida_na_hora(df):
    """2 valores do CSV batem com a aba lida na hora (openpyxl)."""
    rp_15b1 = df[(df["ano"] == 2015) & (df["bimestre"] == 1)][
        "resultado_primario_prev_real_dez25"
    ].iloc[0]
    assert rp_15b1 == pytest.approx(
        _valor_aba("15b1", "Resultado Primário + Previdenciário")
    )

    dcl_25b6 = df[(df["ano"] == 2025) & (df["bimestre"] == 6)]["dcl_real_dez25"].iloc[0]
    assert dcl_25b6 == pytest.approx(_valor_aba("25b6", "DCL"))

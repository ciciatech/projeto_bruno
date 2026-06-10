"""Testes do deflator IPCA base fixa (T28).

Valida ``pipeline.transform.deflator`` contra a aba "Índice de inflação" da
planilha do Prof. Paulo (fixture ``data/deflator_paulo_dez25.csv``, 66 obs,
15b1 → 25b6). Os testes NÃO dependem de rede: leem o IPCA do cache local
``pipeline/data/ipca_433_cache.csv``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.transform.deflator import (
    BASE_PADRAO,
    IPCA_CACHE_CSV,
    deflator_bimestral,
    deflator_mensal,
    indice_ipca_acumulado,
    obter_ipca_mensal,
)

FIXTURE_PAULO = Path(__file__).parent / "data" / "deflator_paulo_dez25.csv"


def test_cache_ipca_existe_para_rodar_offline():
    """O cache do IPCA precisa existir — garante testes sem rede."""
    assert IPCA_CACHE_CSV.exists(), (
        f"Cache ausente: {IPCA_CACHE_CSV}. Gere com "
        "`python3 -m pipeline.transform.deflator` a partir da raiz do repo."
    )
    ipca = obter_ipca_mensal()
    assert {"data", "ipca_mensal"} == set(ipca.columns)
    assert len(ipca) >= 132  # 2015-01 a 2025-12
    assert ipca["data"].is_monotonic_increasing
    assert ipca["ipca_mensal"].notna().all()


def test_deflator_bimestral_bate_com_planilha_paulo():
    """66 bimestres (15b1–25b6) com erro relativo máximo < 0,1% vs Prof. Paulo.

    Na prática a convenção é exata (ruído de ponto flutuante ~2e-15); a
    asserção fina pega regressão de convenção (ex.: trocar último mês por média).
    """
    assert FIXTURE_PAULO.exists(), f"Fixture ausente: {FIXTURE_PAULO}"
    fx = pd.read_csv(FIXTURE_PAULO)
    assert len(fx) == 66

    calc = deflator_bimestral(base="2025-12")
    m = fx.merge(calc, on=["ano", "bimestre"], how="left", validate="1:1")
    assert m["deflator"].notna().all(), "Bimestres do Paulo ausentes no cálculo"

    erro_rel = (m["deflator"] - m["deflator_paulo"]).abs() / m["deflator_paulo"]
    assert erro_rel.max() < 1e-3, f"Erro relativo máximo {erro_rel.max():.2e} >= 0,1%"
    assert erro_rel.max() < 1e-12, (
        f"Convenção divergente da planilha (erro {erro_rel.max():.2e}); "
        "esperado apenas ruído de ponto flutuante"
    )


def test_deflator_bimestre_base_igual_a_um():
    """No bimestre que termina no mês-base, o deflator vale exatamente 1.0."""
    calc = deflator_bimestral(base=BASE_PADRAO)  # 2025-12 → 25b6
    base_25b6 = calc[(calc["ano"] == 2025) & (calc["bimestre"] == 6)]["deflator"].item()
    assert base_25b6 == pytest.approx(1.0, abs=1e-12)


def test_deflator_decrescente_no_acumulado():
    """Inflação acumulada positiva: deflatores >= 1 e caindo ao longo dos anos.

    A série não é estritamente monotônica mês a mês (há bimestres de deflação,
    ex.: 22b4), então testamos a tendência: média anual estritamente decrescente
    e extremos coerentes.
    """
    calc = deflator_bimestral(base="2025-12")
    assert (calc["deflator"] >= 1.0 - 1e-12).all()
    assert calc.iloc[0]["deflator"] == calc["deflator"].max()  # 15b1 é o maior
    assert calc.iloc[-1]["deflator"] == pytest.approx(calc["deflator"].min())

    media_anual = calc.groupby("ano")["deflator"].mean()
    assert media_anual.is_monotonic_decreasing
    assert media_anual.diff().dropna().lt(0).all()  # estritamente decrescente


def test_deflator_mensal_consistente_com_bimestral():
    """O deflator do mês final do bimestre (2b) é idêntico ao bimestral."""
    bim = deflator_bimestral(base="2025-12")
    men = deflator_mensal(base="2025-12")

    men_finais = men[men["mes"] % 2 == 0].copy()
    men_finais["bimestre"] = men_finais["mes"] // 2
    m = bim.merge(
        men_finais[["ano", "bimestre", "deflator"]],
        on=["ano", "bimestre"],
        suffixes=("_bim", "_men"),
        validate="1:1",
    )
    assert len(m) == len(bim)
    assert (m["deflator_bim"] == m["deflator_men"]).all()

    dez25 = men[(men["ano"] == 2025) & (men["mes"] == 12)]["deflator"].item()
    assert dez25 == pytest.approx(1.0, abs=1e-12)


def test_base_parametrizavel_dez24():
    """Base alternativa dez/2024: 24b6 vira 1.0 e a série inteira só muda de escala."""
    d25 = deflator_bimestral(base="2025-12")
    d24 = deflator_bimestral(base="2024-12")

    base_24b6 = d24[(d24["ano"] == 2024) & (d24["bimestre"] == 6)]["deflator"].item()
    assert base_24b6 == pytest.approx(1.0, abs=1e-12)

    razao = d25["deflator"] / d24["deflator"]  # = I(dez/25)/I(dez/24), constante
    assert razao.max() - razao.min() < 1e-12
    assert razao.iloc[0] > 1.0  # inflação positiva em 2025


def test_base_fora_da_serie_levanta_erro():
    """Mês-base fora da cobertura do IPCA (ou formato inválido) deve falhar alto."""
    with pytest.raises(ValueError):
        deflator_bimestral(base="2030-12")
    with pytest.raises(ValueError):
        deflator_mensal(base="bogus")


def test_indice_aceita_ipca_injetado():
    """``indice_ipca_acumulado``/deflatores aceitam IPCA injetado (sem disco/rede)."""
    ipca = pd.DataFrame(
        {
            "data": pd.to_datetime(["2025-09-01", "2025-10-01", "2025-11-01", "2025-12-01"]),
            "ipca_mensal": [0.5, 0.2, 0.18, 0.33],
        }
    )
    indice = indice_ipca_acumulado(ipca)
    calc = deflator_bimestral(base="2025-12", indice=indice)
    b5 = calc[(calc["ano"] == 2025) & (calc["bimestre"] == 5)]["deflator"].item()
    # 25b5 conferido dígito a dígito com a planilha: (1+0.0018)(1+0.0033)
    assert b5 == pytest.approx((1 + 0.0018) * (1 + 0.0033), rel=1e-12)

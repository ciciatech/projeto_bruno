"""Testes de regressão do coletor CAGED municipal (sem rede, dados sintéticos).

Cobre o bug de normalização do código IBGE: a fonte CAGED/PDET entrega o
município com 6 dígitos (sem o dígito verificador) e a versão antiga do
``_agregar_municipal_ce`` concatenava "0" fixo — só 18 dos 184 municípios CE
têm DV=0, então 166 municípios eram descartados silenciosamente pelo filtro
``isin``. O fix mapeia prefixo-6 → código oficial de 7 dígitos.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from pipeline.extract.caged_municipal import _agregar_municipal_ce


def _df_sintetico(municipios: list[str]) -> pd.DataFrame:
    """Monta DataFrame no formato pós-parse do CAGED: 1 admissão + 1 desligamento
    por município (saldo 0, 2 movimentações)."""
    linhas = []
    for m in municipios:
        linhas.append({"municipio": m, "saldo_movimentacao": 1, "salario": 1500.0})
        linhas.append({"municipio": m, "saldo_movimentacao": -1, "salario": 2500.0})
    return pd.DataFrame(linhas)


def test_normaliza_6_digitos_com_dv_diferente_de_zero():
    """Regressão do bug: Abaiara chega como 230010 e o DV correto é 1.

    Com o código antigo (concatenar "0") viraria 2300100 — inválido — e a
    linha seria descartada. Com o fix, deve sair como 2300101.
    """
    out = _agregar_municipal_ce(_df_sintetico(["230010"]), 2024, 3)

    assert not out.empty, "Abaiara (DV != 0) foi descartada — bug do '0' fixo"
    assert out["cod_ibge"].tolist() == ["2300101"]
    assert out["regiao_nome"].iloc[0] == "Cariri"


def test_normaliza_6_digitos_com_dv_zero():
    """Fortaleza (230440 → 2304400, DV=0) já funcionava por coincidência e
    deve continuar funcionando."""
    out = _agregar_municipal_ce(_df_sintetico(["230440"]), 2024, 3)

    assert out["cod_ibge"].tolist() == ["2304400"]
    assert out["regiao_nome"].iloc[0] == "Grande Fortaleza"


def test_codigo_ja_em_7_digitos_passa_intacto():
    """Código IBGE completo (Novo CAGED) não deve ser alterado."""
    out = _agregar_municipal_ce(_df_sintetico(["2300150"]), 2024, 3)  # Acarape

    assert out["cod_ibge"].tolist() == ["2300150"]


def test_todos_os_formatos_juntos_nenhum_descartado():
    """Mistura 6 dígitos (DV != 0 e DV = 0) e 7 dígitos: TODOS devem sair com
    o código de 7 dígitos correto — sem perda silenciosa."""
    out = _agregar_municipal_ce(
        _df_sintetico(["230010", "230440", "2300150"]), 2024, 3
    )

    assert sorted(out["cod_ibge"]) == ["2300101", "2300150", "2304400"]
    # Agregação: 1 admissão + 1 desligamento por município
    assert (out["admissoes"] == 1).all()
    assert (out["desligamentos"] == 1).all()
    assert (out["saldo"] == 0).all()
    assert (out["total_movimentacoes"] == 2).all()
    assert (out["salario_medio"] == 2000.0).all()
    assert (out["ano"] == 2024).all()
    assert (out["mes"] == 3).all()


def test_codigo_desconhecido_descartado_com_warning(caplog: pytest.LogCaptureFixture):
    """Código que começa com 23 mas não é município CE (ex.: 239999) deve ser
    descartado COM warning — a perda silenciosa escondeu o bug por 40 dias."""
    with caplog.at_level(logging.WARNING, logger="pipeline.extract.caged_municipal"):
        out = _agregar_municipal_ce(
            _df_sintetico(["230010", "239999"]), 2024, 3
        )

    # O válido sobrevive; o desconhecido sai
    assert out["cod_ibge"].tolist() == ["2300101"]

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "descarte de código desconhecido deve logar warning"
    msg = " ".join(r.getMessage() for r in warnings)
    assert "239999" in msg
    assert "2 linhas" in msg  # 2 linhas sintéticas do código desconhecido


def test_df_vazio_ou_sem_coluna_municipio_retorna_vazio():
    """Entradas degeneradas retornam DataFrame vazio sem explodir."""
    assert _agregar_municipal_ce(pd.DataFrame(), 2024, 1).empty
    assert _agregar_municipal_ce(None, 2024, 1).empty
    assert _agregar_municipal_ce(
        pd.DataFrame({"outra_coluna": [1]}), 2024, 1
    ).empty


def test_mes_vazio_nao_e_cacheado(tmp_path, monkeypatch):
    """Falha de download/extração devolve df vazio — o mês NÃO pode ir pro cache,
    senão o resume o pula para sempre (regressão do incidente de 2026-06-10)."""
    import pandas as pd

    from pipeline.extract import caged_municipal

    monkeypatch.setattr(caged_municipal, "PROCESSED_DIR", tmp_path)
    monkeypatch.setattr(
        caged_municipal.CagedRais,
        "_processar_caged_antigo_mes",
        staticmethod(lambda ano, mes: pd.DataFrame()),
    )

    caged_municipal.coletar_ce(ano_inicio=2015, ano_fim=2015)

    cache_dir = tmp_path / "caged_municipal" / "_meses_consolidados"
    assert list(cache_dir.glob("*.csv")) == []

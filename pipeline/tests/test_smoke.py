"""Smoke tests do pipeline.

Garante que módulos críticos importam, que o mapeamento de regiões está
íntegro e que o painel regional CE (quando presente) tem schema esperado.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


def test_import_pipeline_run():
    """Smoke do orquestrador principal."""
    from pipeline import run as run_mod

    assert hasattr(run_mod, "PipelineColeta")
    assert hasattr(run_mod, "MODULOS_CE")
    assert isinstance(run_mod.MODULOS_CE, list)
    assert len(run_mod.MODULOS_CE) >= 8


def test_import_coletor_siconfi_invest_municipal():
    """Garante que o coletor SICONFI invest_municipal está acessível."""
    from pipeline.extract import invest_municipal_siconfi as mod

    assert hasattr(mod, "coletar_ce")
    assert callable(mod.coletar_ce)


def test_regioes_ce_184_municipios_14_regioes():
    """O mapeamento canônico precisa ter exatamente 184 municípios e 14 regiões."""
    from pipeline.regioes_ce import REGIOES_CE, get_regiao_info

    info = get_regiao_info()
    assert len(info) == 184, f"Esperados 184 municípios, achei {len(info)}"
    assert info["regiao_codigo"].nunique() == 14
    assert set(REGIOES_CE.keys()) == set(info["regiao_codigo"].unique())


def test_agregar_para_regiao_robusto_quando_df_ja_tem_regiao_codigo():
    """Regressão do bug fixado em commit 7819802 (CAGED quebrava painel).

    Quando o df de entrada já traz regiao_codigo/regiao_nome, o agregar não
    deve quebrar com KeyError no groupby.
    """
    from pipeline.regioes_ce import agregar_para_regiao, get_regiao_info

    info = get_regiao_info()
    sample_cod = info["cod_ibge"].iloc[0]

    df = pd.DataFrame(
        {
            "cod_ibge": [sample_cod, sample_cod],
            "regiao_codigo": ["XX", "YY"],
            "regiao_nome": ["Região errada", "Outra errada"],
            "ano": [2024, 2024],
            "mes": [1, 2],
            "valor": [100.0, 200.0],
        }
    )
    out = agregar_para_regiao(df, "cod_ibge", ["valor"], cols_groupby_extra=["ano", "mes"])

    assert "regiao_codigo" in out.columns
    assert "regiao_nome" in out.columns
    assert (out["regiao_codigo"] != "XX").all()
    assert out["valor"].sum() == 300.0


@pytest.mark.skipif(
    not Path("dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv").exists(),
    reason="painel ainda não gerado (rode python -m pipeline.run --painel-ce)",
)
def test_painel_regional_ce_schema():
    """Painel modelo-pronto precisa ter 14 regiões × 132 meses = 1848 linhas."""
    df = pd.read_csv(
        "dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv",
        dtype={"regiao_codigo": str},
    )
    assert {"regiao_codigo", "regiao_nome", "ano", "mes"}.issubset(df.columns)
    assert df["regiao_codigo"].nunique() == 14
    assert len(df) == 1848
    assert df["ano"].min() == 2015

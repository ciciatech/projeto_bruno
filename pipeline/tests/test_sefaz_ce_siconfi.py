"""Testes do coletor SICONFI Cota-Parte ICMS/IPVA (sefaz_ce_siconfi).

Sem rede: ``Siconfi.coletar_rreo`` é substituído por fake via monkeypatch.
Cobrem o checkpoint/resume (pular pares já coletados, acrescentar novos sem
duplicar) e o default de anos (2015–2025, sem o exercício corrente).
"""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline.extract import sefaz_ce_siconfi as mod

FORTALEZA = "2304400"
ABAIARA = "2300101"


def _fake_payload() -> pd.DataFrame:
    """Payload mínimo no formato da API SICONFI (RREO Anexo 03).

    Duas colunas mensais: <MR> (mês 12) e <MR-1> (mês 11).
    """
    linhas = []
    for coluna, icms, ipva in [("<MR>", 100.0, 10.0), ("<MR-1>", 200.0, 20.0)]:
        linhas.append(
            {"anexo": mod.ANEXO, "conta": "Cota-Parte do ICMS", "coluna": coluna, "valor": icms}
        )
        linhas.append(
            {"anexo": mod.ANEXO, "conta": "Cota-Parte do IPVA", "coluna": coluna, "valor": ipva}
        )
    return pd.DataFrame(linhas)


def _seed_fortaleza_2024(csv_path) -> pd.DataFrame:
    """Grava CSV acumulado com 12 meses de Fortaleza/2024 (estado pré-rodada)."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "cod_ibge": [FORTALEZA] * 12,
            "regiao_codigo": ["03"] * 12,
            "regiao_nome": ["Grande Fortaleza"] * 12,
            "ano": [2024] * 12,
            "mes": list(range(1, 13)),
            "icms": [1000.0] * 12,
            "ipva": [100.0] * 12,
            "total": [1100.0] * 12,
        }
    )
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return df


@pytest.fixture
def csv_tmp(tmp_path, monkeypatch):
    """Redireciona o PROCESSED_DIR (coletor e save_dataframe) para tmp_path."""
    monkeypatch.setattr("pipeline.utils.PROCESSED_DIR", tmp_path)
    monkeypatch.setattr("pipeline.extract.sefaz_ce_siconfi.PROCESSED_DIR", tmp_path)
    return tmp_path / "sefaz_ce_siconfi" / "transf_estaduais_ce_mensal.csv"


@pytest.fixture
def rreo_fake(monkeypatch):
    """Substitui Siconfi.coletar_rreo por fake que registra as chamadas."""
    chamadas: list[tuple[str, int, str | None]] = []

    def fake(ano, periodo, cod_ibge, uf, anexo=None):
        chamadas.append((str(cod_ibge), int(ano), anexo))
        return _fake_payload()

    monkeypatch.setattr(mod.Siconfi, "coletar_rreo", fake)
    return chamadas


def test_resume_pula_pares_ja_presentes(csv_tmp, rreo_fake):
    """(a) Pares (cod_ibge, ano) já no CSV não geram requests e são preservados."""
    seed = _seed_fortaleza_2024(csv_tmp)

    df = mod.coletar_ce(anos=[2024], municipios=[FORTALEZA], throttle=0)

    assert rreo_fake == [], "resume não pode recoletar par já presente no CSV"
    assert len(df) == 12
    salvo = pd.read_csv(csv_tmp, encoding="utf-8-sig", dtype={"cod_ibge": str})
    assert len(salvo) == len(seed) == 12
    assert salvo["cod_ibge"].unique().tolist() == [FORTALEZA]
    assert salvo["icms"].sum() == seed["icms"].sum()


def test_novos_registros_sem_duplicar_antigos(csv_tmp, rreo_fake):
    """(b) Município novo entra no CSV; os registros antigos seguem intactos."""
    _seed_fortaleza_2024(csv_tmp)

    df = mod.coletar_ce(anos=[2024], municipios=[FORTALEZA, ABAIARA], throttle=0)

    # Só Abaiara foi à API, com o filtro de anexo da query
    assert rreo_fake == [(ABAIARA, 2024, mod.ANEXO)]

    salvo = pd.read_csv(
        csv_tmp, encoding="utf-8-sig", dtype={"cod_ibge": str, "regiao_codigo": str}
    )
    assert len(salvo) == 12 + 2  # 12 Fortaleza (seed) + 2 meses fake Abaiara
    assert len(df) == len(salvo)
    assert not salvo.duplicated(subset=["cod_ibge", "ano", "mes"]).any()

    fortaleza = salvo[salvo["cod_ibge"] == FORTALEZA]
    assert len(fortaleza) == 12
    assert fortaleza["icms"].sum() == 12 * 1000.0

    abaiara = salvo[salvo["cod_ibge"] == ABAIARA]
    assert sorted(abaiara["mes"]) == [11, 12]
    assert abaiara["icms"].sum() == 300.0
    assert abaiara["ipva"].sum() == 30.0
    assert (abaiara["total"] == abaiara["icms"] + abaiara["ipva"]).all()
    assert abaiara["regiao_nome"].unique().tolist() == ["Cariri"]


def test_anos_default_2015_a_2025(csv_tmp, rreo_fake):
    """(c) Default de anos exclui o exercício corrente: 2015–2025."""
    assert mod._anos_default() == list(range(2015, 2026))
    assert 2026 not in mod._anos_default()

    # E é o default efetivamente usado por coletar_ce (anos=None)
    mod.coletar_ce(municipios=[ABAIARA], throttle=0)
    anos_chamados = sorted(ano for _, ano, _ in rreo_fake)
    assert anos_chamados == list(range(2015, 2026))

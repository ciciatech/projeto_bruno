"""Testes do coletor SIOF relatório 544 — região × elemento 51/52 (siof_regiao).

Sem rede: as fixtures XLS em ``pipeline/tests/data/siof_regiao/`` são downloads
reais do SIOF-Web (mês=12), uma por esquema regional (2015 = 8 macrorregiões
antigas; 2020 = 14 regiões LC 154/2015). Cobrem parser, dual-esquema, diff de
fluxo bimestral, sanidade de acumulados e a validação contra o gabarito do
spike (``docs/sci01-spike/siof_544_consolidado_2015_2025.csv``).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.extract import siof_regiao as mod
from pipeline.regioes_ce import REGIOES_CE

DATA_DIR = Path(__file__).parent / "data" / "siof_regiao"
FIXTURE_2020_OBRAS = DATA_DIR / "siof_544_2020_12_obras.xls"
FIXTURE_2015_EQUIP = DATA_DIR / "siof_544_2015_12_equip.xls"


# ==============================================================================
# Parser — esquema novo (14 regiões, 2016+)
# ==============================================================================


def test_parse_novo_esquema_14_regioes():
    df = mod.parse_xls_544(FIXTURE_2020_OBRAS.read_bytes(), 2020, 12, "obras")

    assert len(df) == 15  # 14 regiões + Estado do Ceará (não regionalizado)
    assert list(df["regiao_codigo"]) == [f"{i:02d}" for i in range(1, 15)] + ["15"]

    # nomes EXATAMENTE como REGIOES_CE para as 14 regiões
    nomes = dict(zip(df["regiao_codigo"], df["regiao_nome"]))
    for cod, nome in REGIOES_CE.items():
        assert nomes[cod] == nome

    # valores conferidos contra o XLS real (e contra o spike)
    cariri = df[df["regiao_codigo"] == "01"].iloc[0]
    assert cariri["empenhado_acum"] == pytest.approx(410420941.33)
    assert cariri["pago_acum"] == pytest.approx(227222127.03)

    assert df.iloc[0]["ano"] == 2020
    assert df.iloc[0]["mes"] == 12
    assert df.iloc[0]["bimestre"] == 6
    assert df.iloc[0]["elemento"] == "obras"


def test_parse_novo_esquema_estado_ceara_flag():
    df = mod.parse_xls_544(FIXTURE_2020_OBRAS.read_bytes(), 2020, 12, "obras")
    estado = df[df["regiao_codigo"] == "15"].iloc[0]
    # linha não-regionalizada mantida (sem rateio) com flag clara no nome
    assert mod.FLAG_NAO_REGIONALIZADO in estado["regiao_nome"]
    assert estado["empenhado_acum"] == pytest.approx(13028362.15)


# ==============================================================================
# Parser — esquema antigo (8 macrorregiões, 2015)
# ==============================================================================


def test_parse_esquema_antigo_prefixo_a():
    df = mod.parse_xls_544(FIXTURE_2015_EQUIP.read_bytes(), 2015, 12, "equip")

    assert len(df) == 9  # 8 macrorregiões + Estado do Ceará (cód. 22)
    assert list(df["regiao_codigo"]) == [f"A{i}" for i in range(1, 9)] + ["A22"]

    # nomes originais do relatório — NÃO misturar com as 14 regiões
    rmf = df[df["regiao_codigo"] == "A1"].iloc[0]
    assert rmf["regiao_nome"] == "REGIÃO METROPOLITANA DE FORTALEZA"
    assert rmf["empenhado_acum"] == pytest.approx(257553669.60)
    assert rmf["pago_acum"] == pytest.approx(240358904.85)

    estado = df[df["regiao_codigo"] == "A22"].iloc[0]
    assert mod.FLAG_NAO_REGIONALIZADO in estado["regiao_nome"]

    nomes_14 = set(REGIOES_CE.values())
    assert not set(df["regiao_nome"]) & nomes_14


def test_parse_valida_rodape_criterios():
    """O rodapé 'Critérios:' prova que o filtro foi aplicado pelo servidor.

    XLS de obras (elemento 51) declarado como equip (52) deve falhar — é a
    proteção contra o descarte silencioso do filtro (caso do relatório 120 e
    do postback sem a cascata do grupo).
    """
    with pytest.raises(ValueError, match="Critérios"):
        mod.parse_xls_544(FIXTURE_2020_OBRAS.read_bytes(), 2020, 12, "equip")


# ==============================================================================
# Fluxo bimestral (diff do acumulado)
# ==============================================================================


def _acum(ano, bimestre, regiao, emp, pago, elemento="obras"):
    return {
        "ano": ano, "mes": bimestre * 2, "bimestre": bimestre,
        "elemento": elemento, "regiao_codigo": regiao, "regiao_nome": f"R{regiao}",
        "empenhado_acum": emp, "pago_acum": pago,
    }


def test_fluxo_diff_entre_bimestres():
    df = pd.DataFrame(
        [
            _acum(2020, 1, "01", 100.0, 50.0),
            _acum(2020, 2, "01", 250.0, 120.0),
            _acum(2020, 3, "01", 400.0, 300.0),
            # outra região no mesmo ano não contamina o diff
            _acum(2020, 1, "02", 10.0, 5.0),
            _acum(2020, 2, "02", 30.0, 15.0),
        ]
    )
    out = mod.calcular_fluxos(df)

    r1 = out[out["regiao_codigo"] == "01"].sort_values("bimestre")
    # bimestre 1 = acumulado de fevereiro
    assert list(r1["empenhado_fluxo"]) == [100.0, 150.0, 150.0]
    assert list(r1["pago_fluxo"]) == [50.0, 70.0, 180.0]

    r2 = out[out["regiao_codigo"] == "02"].sort_values("bimestre")
    assert list(r2["empenhado_fluxo"]) == [10.0, 20.0]


def test_fluxo_nao_cruza_anos_nem_elementos():
    df = pd.DataFrame(
        [
            _acum(2020, 6, "01", 1000.0, 900.0),
            _acum(2021, 1, "01", 80.0, 40.0),
            _acum(2021, 1, "01", 7.0, 3.0, elemento="equip"),
        ]
    )
    out = mod.calcular_fluxos(df)
    b1_2021 = out[(out["ano"] == 2021) & (out["elemento"] == "obras")].iloc[0]
    assert b1_2021["empenhado_fluxo"] == 80.0  # não diminui o acumulado de 2020
    b1_equip = out[out["elemento"] == "equip"].iloc[0]
    assert b1_equip["empenhado_fluxo"] == 7.0


def test_fluxo_negativo_e_logado_nao_falha(caplog):
    """Anulações geram fluxo negativo: warning + linha mantida, sem exceção."""
    df = pd.DataFrame(
        [
            _acum(2020, 1, "01", 100.0, 50.0),
            _acum(2020, 2, "01", 90.0, 60.0),  # empenhado caiu (anulação)
        ]
    )
    out = mod.calcular_fluxos(df)
    with caplog.at_level("WARNING"):
        negativos = mod.checar_sanidade_acumulados(out)
    assert len(negativos) == 1
    assert negativos.iloc[0]["empenhado_fluxo"] == -10.0
    assert any("negativo" in r.message for r in caplog.records)


# ==============================================================================
# Validação contra o gabarito do spike (via CSV, sem rede)
# ==============================================================================


@pytest.fixture
def df_anual_do_spike():
    """Reconstrói um df_anual no formato do coletor a partir do spike."""
    spike = pd.read_csv(mod.SPIKE_CSV, dtype={"cod": str})
    df = pd.DataFrame(
        {
            "ano": spike["ano"],
            "mes": 12,
            "bimestre": 6,
            "elemento": spike["elemento"],
            "regiao_codigo": [
                f"A{int(c)}" if a <= 2015 else c.zfill(2)
                for a, c in zip(spike["ano"], spike["cod"])
            ],
            "regiao_nome": spike["regiao"],
            "empenhado_acum": spike["empenhado"],
            "pago_acum": spike["pago"],
        }
    )
    return df


def test_validar_spike_sem_divergencias(df_anual_do_spike):
    divergencias = mod.validar_contra_spike(df_anual_do_spike)
    assert divergencias.empty


def test_validar_spike_detecta_valor_divergente(df_anual_do_spike):
    df = df_anual_do_spike.copy()
    idx = df[(df["ano"] == 2020) & (df["elemento"] == "obras")].index[0]
    df.loc[idx, "empenhado_acum"] *= 1.001  # fora do rtol 1e-6
    divergencias = mod.validar_contra_spike(df)
    assert len(divergencias) == 1
    assert divergencias.iloc[0]["campo"] == "empenhado"
    assert divergencias.iloc[0]["ano"] == 2020


def test_validar_spike_detecta_celula_faltante(df_anual_do_spike):
    df = df_anual_do_spike[
        ~((df_anual_do_spike["ano"] == 2015) & (df_anual_do_spike["regiao_codigo"] == "A22"))
    ]
    divergencias = mod.validar_contra_spike(df)
    cobertura = divergencias[divergencias["campo"] == "cobertura"]
    assert len(cobertura) == 2  # equip e obras de 2015/cód. 22
    assert set(cobertura["cod"]) == {"22"}


def test_fixture_2020_bate_com_spike():
    """Parser + gabarito ponta a ponta: o XLS real de 2020 reproduz o spike."""
    df = mod.parse_xls_544(FIXTURE_2020_OBRAS.read_bytes(), 2020, 12, "obras")
    spike = pd.read_csv(mod.SPIKE_CSV, dtype={"cod": str})
    alvo = spike[(spike["ano"] == 2020) & (spike["elemento"] == "obras")]
    merged = alvo.merge(
        df, left_on="cod", right_on="regiao_codigo", how="inner", validate="1:1"
    )
    assert len(merged) == len(alvo) == 15
    pd.testing.assert_series_equal(
        merged["empenhado"], merged["empenhado_acum"],
        check_names=False, rtol=1e-6,
    )
    pd.testing.assert_series_equal(
        merged["pago"], merged["pago_acum"], check_names=False, rtol=1e-6
    )

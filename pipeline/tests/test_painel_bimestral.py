"""Testes do painel bimestral regional CE (T27).

Sem rede: usam o painel mensal já em disco
(``dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv``) e o
cache local do IPCA (``pipeline/data/ipca_433_cache.csv``) do T28.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pipeline.config import PERIODO_FIM_MENSAL, PERIODO_INICIO_MENSAL
from pipeline.regioes_ce import REGIOES_CE
from pipeline.transform.deflator import IPCA_CACHE_CSV
from pipeline.transform.painel_bimestral import (
    LIMIAR_OUTLIER_MENSAL,
    PAINEL_MENSAL_CSV,
    VARIAVEIS_XLSX,
    aplicar_gate_outliers,
    carregar_matriz_regras,
    construir_painel_bimestral,
    detectar_meses_outlier,
    exportar_xlsx_paulo,
)

pytestmark = pytest.mark.skipif(
    not PAINEL_MENSAL_CSV.exists(),
    reason="painel mensal ainda não gerado (rode python -m pipeline.run --painel-ce)",
)


@pytest.fixture(scope="module")
def painel_mensal() -> pd.DataFrame:
    return pd.read_csv(PAINEL_MENSAL_CSV, dtype={"regiao_codigo": str})


@pytest.fixture(scope="module")
def painel_bim() -> pd.DataFrame:
    return construir_painel_bimestral()


def test_shape_14_regioes_x_bimestres(painel_bim):
    """nº de linhas = 14 regiões × 6 bimestres × anos do período; sem região faltante."""
    n_anos = PERIODO_FIM_MENSAL - PERIODO_INICIO_MENSAL + 1
    assert len(painel_bim) == 14 * 6 * n_anos
    assert painel_bim["regiao_codigo"].nunique() == 14
    assert set(painel_bim["regiao_nome"]) == set(REGIOES_CE.values())
    assert set(painel_bim["bimestre"]) == {1, 2, 3, 4, 5, 6}
    # cada região cobre todos os bimestres do período
    por_regiao = painel_bim.groupby("regiao_codigo").size()
    assert (por_regiao == 6 * n_anos).all()
    # chave única
    assert not painel_bim.duplicated(["regiao_codigo", "ano", "bimestre"]).any()


def test_matriz_regras_cobre_todas_as_colunas(painel_mensal):
    """Toda coluna de valor do painel mensal tem regra na matriz regional (e vice-versa)."""
    regras = carregar_matriz_regras()
    cols_valor = [
        c
        for c in painel_mensal.columns
        if c not in {"regiao_codigo", "regiao_nome", "ano", "mes"}
    ]
    assert set(regras["variavel"]) == set(cols_valor)


@pytest.mark.parametrize("coluna", ["saldo", "bf_valor_total"])
def test_consistencia_soma_anual_mensal_vs_bimestral(painel_mensal, painel_bim, coluna):
    """Soma anual por região no bimestral == soma anual no mensal (fluxos)."""
    anual_mensal = painel_mensal.groupby(["regiao_codigo", "ano"])[coluna].sum(
        min_count=1
    )
    anual_bim = painel_bim.groupby(["regiao_codigo", "ano"])[coluna].sum(min_count=1)
    m = pd.concat(
        [anual_mensal.rename("mensal"), anual_bim.rename("bimestral")], axis=1
    )
    # NaN estrutural (ex.: regiões sem CAGED) deve permanecer NaN dos dois lados
    assert (m["mensal"].isna() == m["bimestral"].isna()).all()
    ok = m.dropna()
    assert len(ok) > 0
    assert np.allclose(ok["mensal"], ok["bimestral"], rtol=1e-9)


def test_soma_nao_zera_regioes_sem_caged(painel_bim):
    """Regiões 100% sem CAGED na fonte ficam NaN no bimestral (não 0)."""
    cariri = painel_bim[painel_bim["regiao_nome"] == "Cariri"]
    assert cariri["saldo"].isna().all()


def test_colunas_real_existem_e_batem_com_deflator_manual(painel_bim):
    """*_real = nominal × deflator; spot-check com deflator calculado à mão do cache IPCA."""
    regras = carregar_matriz_regras()
    monetarias = regras.loc[regras["deflacionamento"] == "ipca", "variavel"].tolist()
    assert len(monetarias) == 20
    for col in monetarias:
        assert f"{col}_real" in painel_bim.columns, f"falta {col}_real"

    # deflator manual de 2024-B6: I(2025-12) / I(2024-12), cumprod do cache IPCA
    ipca = pd.read_csv(IPCA_CACHE_CSV)
    ipca["data"] = pd.to_datetime(ipca["data"])
    indice = (ipca.set_index("data")["ipca_mensal"] / 100.0 + 1.0).cumprod()
    defl_24b6 = indice.loc["2025-12-01"] / indice.loc["2024-12-01"]

    cel = painel_bim[
        (painel_bim["regiao_nome"] == "Grande Fortaleza")
        & (painel_bim["ano"] == 2024)
        & (painel_bim["bimestre"] == 6)
    ].iloc[0]
    assert cel["bf_valor_total_real"] == pytest.approx(
        cel["bf_valor_total"] * defl_24b6, rel=1e-12
    )
    assert cel["deflator"] == pytest.approx(defl_24b6, rel=1e-12)

    # no bimestre-base (2025-B6) deflator == 1 → real == nominal
    base = painel_bim[
        (painel_bim["regiao_nome"] == "Grande Fortaleza")
        & (painel_bim["ano"] == 2025)
        & (painel_bim["bimestre"] == 6)
    ].iloc[0]
    assert base["deflator"] == pytest.approx(1.0, abs=1e-12)
    assert base["bf_valor_total_real"] == pytest.approx(
        base["bf_valor_total"], rel=1e-12
    )


def test_salario_medio_ponderado_por_movimentacoes(painel_mensal, painel_bim):
    """salario_medio bimestral = média dos meses ponderada por total_movimentacoes."""
    sub = painel_mensal[
        (painel_mensal["regiao_codigo"] == "03")  # Grande Fortaleza
        & (painel_mensal["ano"] == 2024)
        & (painel_mensal["mes"].isin([1, 2]))
    ]
    assert sub["salario_medio"].notna().all(), "fixture: esperava CAGED em 2024 jan-fev"
    peso = sub["total_movimentacoes"]
    esperado = (sub["salario_medio"] * peso).sum() / peso.sum()

    obtido = painel_bim[
        (painel_bim["regiao_codigo"] == "03")
        & (painel_bim["ano"] == 2024)
        & (painel_bim["bimestre"] == 1)
    ]["salario_medio"].item()
    assert obtido == pytest.approx(esperado, rel=1e-12)
    # média simples seria diferente (pesos distintos nos 2 meses) — garante a ponderação
    assert obtido != pytest.approx(sub["salario_medio"].mean(), rel=1e-6)


def test_ultimo_mes_para_anuais_replicados(painel_mensal, painel_bim):
    """siof_anual_* e pop usam último (valor anual replicado — soma dobraria)."""
    mensal_gf_26 = painel_mensal[
        (painel_mensal["regiao_codigo"] == "03")
        & (painel_mensal["ano"] == 2026)
        & (painel_mensal["mes"] == 2)
    ]["siof_anual_pago"].item()
    bim_gf_26b1 = painel_bim[
        (painel_bim["regiao_codigo"] == "03")
        & (painel_bim["ano"] == 2026)
        & (painel_bim["bimestre"] == 1)
    ]["siof_anual_pago"].item()
    assert bim_gf_26b1 == pytest.approx(mensal_gf_26)  # último, NÃO 2× o valor


def test_gate_detecta_apenas_set2024(painel_mensal):
    """No painel real, o gate flagra exatamente set/2024 (transf_fed_*, erro ~100x).

    Qualquer outro mês flagrado = falso positivo (regressão de calibração);
    set/2024 não flagrado = o outlier de R$ 136,9 bi volta ao entregável.
    """
    regras = carregar_matriz_regras()
    auditoria = detectar_meses_outlier(painel_mensal, regras)
    assert not auditoria.empty
    assert set(zip(auditoria["ano"], auditoria["mes"])) == {(2024, 9)}
    assert set(auditoria["fonte"]) == {"transferencias_municipais_stn"}
    assert "transf_fed_total" in set(auditoria["variavel"])
    assert (auditoria["razao"] > LIMIAR_OUTLIER_MENSAL).all()


def test_gate_anula_familia_da_fonte_no_mensal(painel_mensal):
    """set/2024 vira NaN em TODAS as transf_fed_* (inclusive as séries miúdas);
    meses vizinhos e outras fontes ficam intactos."""
    regras = carregar_matriz_regras()
    limpo, auditoria = aplicar_gate_outliers(painel_mensal, regras)
    assert not auditoria.empty

    set24 = limpo[(limpo["ano"] == 2024) & (limpo["mes"] == 9)]
    for col in [
        "transf_fed_fpm_dest",
        "transf_fed_fundeb_dest",
        "transf_fed_itr_dest",
        "transf_fed_outros_dest",
        "transf_fed_royalties_dest",
        "transf_fed_total",
    ]:
        assert set24[col].isna().all(), f"{col} deveria estar anulada em set/2024"

    out24 = limpo[(limpo["ano"] == 2024) & (limpo["mes"] == 10)]
    assert out24["transf_fed_total"].notna().any()

    orig_set24 = painel_mensal[
        (painel_mensal["ano"] == 2024) & (painel_mensal["mes"] == 9)
    ]
    assert set24["bf_valor_total"].equals(orig_set24["bf_valor_total"])
    # e o painel original não foi modificado in place
    assert orig_set24["transf_fed_total"].notna().any()


def test_bimestre_24b5_anulado_no_painel(painel_bim):
    """O bimestre que contém o mês-outlier fica NaN (nominal e real) em todas
    as regiões — somar só outubro viraria meio-bimestre disfarçado."""
    b5 = painel_bim[(painel_bim["ano"] == 2024) & (painel_bim["bimestre"] == 5)]
    assert len(b5) == 14
    assert b5["transf_fed_total"].isna().all()
    assert b5["transf_fed_total_real"].isna().all()
    assert b5["transf_fed_fpm_dest"].isna().all()
    # vizinhos preservados
    b4 = painel_bim[(painel_bim["ano"] == 2024) & (painel_bim["bimestre"] == 4)]
    b6 = painel_bim[(painel_bim["ano"] == 2024) & (painel_bim["bimestre"] == 6)]
    assert b4["transf_fed_total"].notna().all()
    assert b6["transf_fed_total"].notna().all()
    # outras fontes intactas no próprio 24b5
    assert b5["bf_valor_total"].notna().all()


def test_gate_sintetico_erro_de_unidade():
    """Gate em dados sintéticos: série limpa não flagra; mês 100x é anulado."""
    mensal = pd.DataFrame(
        {
            "regiao_codigo": "01",
            "regiao_nome": "Regiao A",
            "ano": 2024,
            "mes": list(range(1, 13)),
            "bf_valor_total": [1e8] * 12,
        }
    )
    regras = pd.DataFrame(
        {
            "variavel": ["bf_valor_total"],
            "fonte": ["bolsa_familia"],
            "regra_bimestral": ["soma"],
            "deflacionamento": ["ipca"],
        }
    )
    assert detectar_meses_outlier(mensal, regras).empty

    mensal.loc[mensal["mes"] == 9, "bf_valor_total"] = 1e10  # erro de unidade 100x
    limpo, auditoria = aplicar_gate_outliers(mensal, regras)
    assert len(auditoria) == 1
    assert (auditoria.iloc[0]["ano"], auditoria.iloc[0]["mes"]) == (2024, 9)
    assert auditoria.iloc[0]["razao"] == pytest.approx(100.0)
    assert limpo.loc[limpo["mes"] == 9, "bf_valor_total"].isna().all()
    assert limpo.loc[limpo["mes"] != 9, "bf_valor_total"].notna().all()


def test_xlsx_24b5_vazio_mas_visivel(tmp_path, painel_bim):
    """Na aba transf_fed_total o período 24b5 aparece (não some) e está vazio."""
    pytest.importorskip("openpyxl")
    out = exportar_xlsx_paulo(painel_bim, path=tmp_path / "painel_gate.xlsx")
    tf = pd.read_excel(out, sheet_name="transf_fed_total", index_col=0)
    assert "24b5" in tf.index, "linha 24b5 não pode sumir silenciosamente da aba"
    assert tf.loc["24b5"].isna().all()
    assert tf.loc["24b4"].notna().all()
    assert tf.loc["24b6"].notna().all()


def test_leia_me_documenta_ressalvas(tmp_path, painel_bim):
    """Leia-me declara estágio EMPENHADO do invest_mun_valor, cobertura parcial
    do CAGED (18/184) e o gate de outliers (24b5 anulado)."""
    openpyxl = pytest.importorskip("openpyxl")
    out = exportar_xlsx_paulo(painel_bim, path=tmp_path / "painel_leiame.xlsx")
    wb = openpyxl.load_workbook(out, read_only=True)
    ws = wb["Leia-me"]
    texto = "\n".join(
        str(cell.value)
        for row in ws.iter_rows()
        for cell in row
        if cell.value is not None
    )
    wb.close()
    assert "EMPENHADAS" in texto  # estágio da despesa (finding empenhado vs pago)
    assert "18 dos" in texto and "184" in texto  # cobertura parcial CAGED
    assert "24b5" in texto  # gate de outliers documentado
    assert "Estoque de empregos" in texto  # não comparável ao gabarito


def test_exportar_xlsx_paulo(tmp_path, painel_bim):
    """xlsx: aba Leia-me + 1 aba por variável-chave, regiões nas colunas, reais nas monetárias."""
    openpyxl = pytest.importorskip("openpyxl")

    out = exportar_xlsx_paulo(painel_bim, path=tmp_path / "painel.xlsx")
    assert out.exists()

    wb = openpyxl.load_workbook(out, read_only=True)
    assert wb.sheetnames[0] == "Leia-me"
    assert set(VARIAVEIS_XLSX).issubset(set(wb.sheetnames))
    wb.close()

    # aba não monetária (saldo): valores nominais, colunas = 14 regiões
    saldo = pd.read_excel(out, sheet_name="saldo", index_col=0)
    assert list(saldo.columns) == list(REGIOES_CE.values())
    esperado = painel_bim[
        (painel_bim["regiao_nome"] == "Grande Fortaleza")
        & (painel_bim["ano"] == 2024)
        & (painel_bim["bimestre"] == 1)
    ]["saldo"].item()
    assert saldo.loc["24b1", "Grande Fortaleza"] == pytest.approx(esperado)

    # aba monetária (bf_valor_total): valores REAIS
    bf = pd.read_excel(out, sheet_name="bf_valor_total", index_col=0)
    esperado_real = painel_bim[
        (painel_bim["regiao_nome"] == "Grande Fortaleza")
        & (painel_bim["ano"] == 2024)
        & (painel_bim["bimestre"] == 6)
    ]["bf_valor_total_real"].item()
    assert bf.loc["24b6", "Grande Fortaleza"] == pytest.approx(esperado_real)

    # siof_anual_pago: só 2026 na fonte → exportado nominal (sem IPCA 2026), não vazio
    siof = pd.read_excel(out, sheet_name="siof_anual_pago", index_col=0)
    assert len(siof) > 0
    assert all(str(p).startswith("26b") for p in siof.index)

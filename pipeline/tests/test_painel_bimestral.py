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
    COLS_SIOF_NATIVO,
    LACUNAS_UPSTREAM_POR_FONTE,
    PAINEL_MENSAL_CSV,
    REGRA_NATIVA_BIMESTRAL,
    SIOF_REGIAO_BIMESTRAL_CSV,
    VARIAVEIS_XLSX,
    aplicar_gate_outliers,
    carregar_matriz_regras,
    carregar_siof_bimestral_nativo,
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
    """Toda coluna de valor do painel mensal tem regra na matriz regional (e
    vice-versa). Exceção documentada (T46/SCI-01): variáveis com regra
    ``nativa_bimestral`` (siof_obras_pago/siof_equip_pago/siof_invest_pago,
    fonte siof_regiao_544) constam na matriz mas NÃO existem no mensal — o
    dado é nativamente bimestral e entra por merge direto no bimestral."""
    regras = carregar_matriz_regras()
    cols_valor = [
        c
        for c in painel_mensal.columns
        if c not in {"regiao_codigo", "regiao_nome", "ano", "mes"}
    ]
    nativas = set(
        regras.loc[regras["regra_bimestral"] == REGRA_NATIVA_BIMESTRAL, "variavel"]
    )
    assert nativas == set(COLS_SIOF_NATIVO)
    assert (
        regras.loc[regras["variavel"].isin(nativas), "fonte"] == "siof_regiao_544"
    ).all(), "séries nativas bimestrais devem ser documentadas com fonte siof_regiao_544"
    assert set(regras["variavel"]) - nativas == set(cols_valor)
    assert not nativas & set(cols_valor), "nativa bimestral não pode existir no mensal"


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


def test_caged_cobre_todas_as_regioes(painel_bim):
    """Recoleta de jun/2026 (bug do DV corrigido): CAGED cobre as 14 regiões.

    Substitui o teste que assumia Cariri 100% NaN — aquilo codificava o bug de
    cobertura (18/184 municípios) da coleta antiga.
    """
    com_saldo = painel_bim.loc[painel_bim["saldo"].notna(), "regiao_nome"]
    assert com_saldo.nunique() == 14


def test_soma_nao_zera_ausencia_estrutural(painel_bim):
    """Ausência estrutural fica NaN no bimestral (não 0): BPC só existe a
    partir de 2019 na fonte — bimestres anteriores não podem virar 0."""
    pre_2019 = painel_bim[painel_bim["ano"] < 2019]
    assert pre_2019["bpc_valor_total"].isna().all()


def test_colunas_real_existem_e_batem_com_deflator_manual(painel_bim):
    """*_real = nominal × deflator; spot-check com deflator calculado à mão do cache IPCA."""
    regras = carregar_matriz_regras()
    monetarias = regras.loc[regras["deflacionamento"] == "ipca", "variavel"].tolist()
    # 27 (20 originais + 7 crédito BNB) + 5 do SIOF rel. 544 (T46/SCI-01):
    # split anual siof_obras_pago_anual/siof_equip_pago_anual + fluxos
    # bimestrais nativos siof_obras_pago/siof_equip_pago/siof_invest_pago.
    assert len(monetarias) == 32
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


# ---------------------------------------------------------------------------
# SIOF rel. 544 — fluxo bimestral nativo + fechamento anual (T46/SCI-01)
# ---------------------------------------------------------------------------

requer_siof_544 = pytest.mark.skipif(
    not SIOF_REGIAO_BIMESTRAL_CSV.exists(),
    reason="siof_regiao_bimestral.csv ausente (rode python3 -m pipeline.extract.siof_regiao)",
)


@requer_siof_544
def test_siof_fluxo_bimestral_nativo_cobertura(painel_bim):
    """siof_obras_pago/siof_equip_pago/siof_invest_pago: 14 regiões × 6
    bimestres × 2016-2025 (840 células), nada fora dessa janela (2015 =
    esquema de 8 macrorregiões, fica NaN até o crosswalk T47; 2026 ainda não
    coletado no 544); invest = obras + equip."""
    for col in COLS_SIOF_NATIVO:
        assert col in painel_bim.columns
        com_dado = painel_bim[painel_bim[col].notna()]
        assert len(com_dado) == 14 * 6 * 10, col
        assert com_dado["regiao_codigo"].nunique() == 14, col
        assert (com_dado["ano"].min(), com_dado["ano"].max()) == (2016, 2025), col
        fora = painel_bim[(painel_bim["ano"] < 2016) | (painel_bim["ano"] > 2025)]
        assert fora[col].isna().all(), f"{col} não pode ter dado fora de 2016-2025"
    dentro = painel_bim[painel_bim["siof_invest_pago"].notna()]
    assert np.allclose(
        dentro["siof_invest_pago"],
        dentro["siof_obras_pago"] + dentro["siof_equip_pago"],
        rtol=1e-12,
    )


@requer_siof_544
def test_siof_fluxo_nativo_bate_com_fonte(painel_bim):
    """O merge direto preserva o pago_fluxo do CSV da coleta (célula a célula
    num spot-check) e o loader exclui o cód. 15 (não regionalizado) e 2015."""
    fonte = pd.read_csv(
        SIOF_REGIAO_BIMESTRAL_CSV, dtype={"regiao_codigo": str}, encoding="utf-8-sig"
    )
    esperado = fonte[
        (fonte["regiao_codigo"] == "01")
        & (fonte["ano"] == 2020)
        & (fonte["bimestre"] == 3)
        & (fonte["elemento"] == "obras")
    ]["pago_fluxo"].item()
    obtido = painel_bim[
        (painel_bim["regiao_codigo"] == "01")
        & (painel_bim["ano"] == 2020)
        & (painel_bim["bimestre"] == 3)
    ]["siof_obras_pago"].item()
    assert obtido == pytest.approx(esperado, rel=1e-12)

    nativo = carregar_siof_bimestral_nativo()
    assert set(nativo["regiao_codigo"]) == set(REGIOES_CE)  # sem "15" nem "A*"
    assert nativo["ano"].min() == 2016


@requer_siof_544
def test_siof_fluxos_bimestrais_fecham_com_o_anual(painel_bim):
    """Coerência entre os DOIS caminhos de integração do 544: a soma dos 6
    fluxos bimestrais do ano (siof_invest_pago, nativo) deve fechar com o
    fechamento anual replicado (siof_anual_pago, via painel mensal) em TODAS
    as 140 células região-ano de 2016-2025."""
    sub = painel_bim[(painel_bim["ano"] >= 2016) & (painel_bim["ano"] <= 2025)]
    soma_fluxos = sub.groupby(["regiao_codigo", "ano"])["siof_invest_pago"].sum(
        min_count=6
    )
    anual = sub[sub["bimestre"] == 6].set_index(["regiao_codigo", "ano"])[
        "siof_anual_pago"
    ]
    m = pd.concat([soma_fluxos.rename("fluxos"), anual.rename("anual")], axis=1)
    assert len(m) == 14 * 10
    assert m.notna().all().all()
    assert np.allclose(m["fluxos"], m["anual"], rtol=1e-6)


@requer_siof_544
def test_siof_anual_cobertura_2016_2026_e_split(painel_mensal):
    """siof_anual_empenhado/pago cobrem 2016-2026 (544 em 2016-2025 + caminho
    corrente 2026, inalterado); 2015 fica NaN; o split por elemento
    (siof_obras_pago_anual + siof_equip_pago_anual) soma o total em 2016-2025."""
    anos_pago = sorted(painel_mensal.loc[painel_mensal["siof_anual_pago"].notna(), "ano"].unique())
    assert anos_pago == list(range(2016, 2027))
    assert painel_mensal.loc[painel_mensal["ano"] == 2015, "siof_anual_pago"].isna().all()
    # caminho 2026 (extração corrente) intacto: dotação/n_acoes só em 2026
    anos_dot = sorted(painel_mensal.loc[painel_mensal["siof_anual_dotacao"].notna(), "ano"].unique())
    assert anos_dot == [2026]

    sub = painel_mensal[(painel_mensal["ano"] >= 2016) & (painel_mensal["ano"] <= 2025)]
    assert sub["siof_obras_pago_anual"].notna().all()
    assert sub["siof_equip_pago_anual"].notna().all()
    assert np.allclose(
        sub["siof_obras_pago_anual"] + sub["siof_equip_pago_anual"],
        sub["siof_anual_pago"],
        rtol=1e-9,
    )
    # split não existe fora de 2016-2025 (2026 vem do caminho corrente, sem split)
    fora = painel_mensal[(painel_mensal["ano"] < 2016) | (painel_mensal["ano"] > 2025)]
    assert fora["siof_obras_pago_anual"].isna().all()


@requer_siof_544
def test_xlsx_abas_siof_544(tmp_path, painel_bim):
    """As abas siof_obras_pago/siof_equip_pago/siof_invest_pago (blocos 4-6 da
    planilha do Prof. Paulo — variável central da tese) saem em valores REAIS
    (R$ dez/2025), janela 16b1-25b6, espelhando as colunas *_real do painel."""
    pytest.importorskip("openpyxl")
    out = exportar_xlsx_paulo(painel_bim, path=tmp_path / "painel_siof.xlsx")
    for var in COLS_SIOF_NATIVO:
        aba = pd.read_excel(out, sheet_name=var, index_col=0)
        assert list(aba.columns) == list(REGIOES_CE.values())
        assert str(aba.index[0]) == "16b1"
        assert str(aba.index[-1]) == "25b6"
        esperado = painel_bim[
            (painel_bim["regiao_nome"] == "Grande Fortaleza")
            & (painel_bim["ano"] == 2024)
            & (painel_bim["bimestre"] == 6)
        ][f"{var}_real"].item()
        assert aba.loc["24b6", "Grande Fortaleza"] == pytest.approx(esperado)


def test_gate_nao_flagra_nenhum_mes_no_painel_corrigido(painel_mensal):
    """Com set/2024 das transf_fed_* corrigido na coleta (fonte publicava
    CENTAVOS; ÷100 aplicado em jun/2026), o painel real não tem mais nenhum
    mês-outlier: a auditoria deve sair VAZIA.

    Qualquer mês flagrado aqui = falso positivo (regressão de calibração) OU
    novo erro de unidade upstream — investigar antes de relaxar o teste.
    A mecânica do gate continua coberta por dados sintéticos em
    ``test_gate_sintetico_erro_de_unidade``.
    """
    regras = carregar_matriz_regras()
    auditoria = detectar_meses_outlier(painel_mensal, regras)
    assert auditoria.empty, (
        f"gate flagrou meses no painel corrigido:\n{auditoria}"
    )


def test_gate_nao_altera_painel_limpo(painel_mensal):
    """Sem outliers, aplicar_gate_outliers devolve os dados intactos (e não
    modifica o painel original in place)."""
    regras = carregar_matriz_regras()
    limpo, auditoria = aplicar_gate_outliers(painel_mensal, regras)
    assert auditoria.empty

    set24 = limpo[(limpo["ano"] == 2024) & (limpo["mes"] == 9)]
    orig_set24 = painel_mensal[
        (painel_mensal["ano"] == 2024) & (painel_mensal["mes"] == 9)
    ]
    # set/2024 corrigido permanece válido (era anulado quando vinha em centavos)
    assert set24["transf_fed_total"].notna().all()
    assert set24["transf_fed_total"].equals(orig_set24["transf_fed_total"])
    assert set24["bf_valor_total"].equals(orig_set24["bf_valor_total"])


def test_bimestre_24b5_valido_no_painel(painel_bim):
    """24b5 (set-out/2024) volta a ser válido após a correção do set/2024:
    não pode mais ser anulado pelo gate, e deve ficar na ordem de grandeza
    dos bimestres vizinhos (sem resíduo do erro de 100x).

    24b6 NÃO serve de vizinho: nov/2024 não foi publicado pela fonte e o
    bimestre é anulado (LACUNAS_UPSTREAM_POR_FONTE) — usamos 24b4 e 25b1.
    """
    b5 = painel_bim[(painel_bim["ano"] == 2024) & (painel_bim["bimestre"] == 5)]
    assert len(b5) == 14
    assert b5["transf_fed_total"].notna().all()
    assert b5["transf_fed_total_real"].notna().all()
    assert b5["transf_fed_fpm_dest"].notna().all()
    # ordem de grandeza coerente com os vizinhos (erro de centavos era ~100x)
    b4 = painel_bim[(painel_bim["ano"] == 2024) & (painel_bim["bimestre"] == 4)]
    b1_25 = painel_bim[(painel_bim["ano"] == 2025) & (painel_bim["bimestre"] == 1)]
    razao_b4 = b5["transf_fed_total"].sum() / b4["transf_fed_total"].sum()
    razao_25b1 = b5["transf_fed_total"].sum() / b1_25["transf_fed_total"].sum()
    assert 0.2 < razao_b4 < 5
    assert 0.2 < razao_25b1 < 5
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


def test_xlsx_24b5_preenchido(tmp_path, painel_bim):
    """Na aba transf_fed_total o período 24b5 aparece E está preenchido —
    set/2024 foi corrigido na coleta (fonte publicava centavos; ÷100), então
    o gate não anula mais o bimestre. Já 24b6 fica VISÍVEL e VAZIO: nov/2024
    nunca foi publicado pela fonte (lacuna upstream documentada) e somar só
    dezembro seria meio-bimestre disfarçado de valor válido."""
    pytest.importorskip("openpyxl")
    out = exportar_xlsx_paulo(painel_bim, path=tmp_path / "painel_gate.xlsx")
    tf = pd.read_excel(out, sheet_name="transf_fed_total", index_col=0)
    assert "24b5" in tf.index
    assert tf.loc["24b5"].notna().all(), "24b5 corrigido não pode voltar a ser anulado"
    assert tf.loc["24b4"].notna().all()
    assert "24b6" in tf.index, "lacuna interior não pode sumir do entregável"
    assert tf.loc["24b6"].isna().all(), "24b6 (nov/2024 não publicado) deve ficar vazio"


def test_leia_me_documenta_ressalvas(tmp_path, painel_bim):
    """Leia-me declara estágio EMPENHADO do invest_mun_valor, cobertura plena
    do CAGED recoletado (184/184), o gate de outliers (caso histórico 24b5
    corrigido) e a convenção do crédito BNB (município da agência)."""
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
    assert "184 dos 184" in texto  # CAGED recoletado: cobertura plena
    assert "24b5" in texto  # gate de outliers documentado (caso histórico)
    assert "Estoque de empregos" in texto  # saldo = fluxo, não comparável
    assert "AGÊNCIA" in texto  # crédito BNB no município da agência


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

    # siof_anual_pago: com o rel. 544 (2016-2025) a aba passa a sair em REAIS.
    # 2015 (esquema de 8 macrorregiões, NaN) e 2026 (sem IPCA do ano → _real
    # NaN; nominal segue no CSV) são extremidades vazias e saem da aba.
    siof = pd.read_excel(out, sheet_name="siof_anual_pago", index_col=0)
    assert str(siof.index[0]) == "16b1"
    assert str(siof.index[-1]) == "25b6"
    esperado_siof = painel_bim[
        (painel_bim["regiao_nome"] == "Grande Fortaleza")
        & (painel_bim["ano"] == 2024)
        & (painel_bim["bimestre"] == 6)
    ]["siof_anual_pago_real"].item()
    assert siof.loc["24b6", "Grande Fortaleza"] == pytest.approx(esperado_siof)


# ---------------------------------------------------------------------------
# Continuidade de meses por fonte (regressão do incidente CAGED 2025-03)
# ---------------------------------------------------------------------------

#: Lacunas interiores ACEITAS por fonte — toda exceção aqui precisa estar
#: documentada na matriz de regras (coluna observacao). Qualquer buraco fora
#: desta lista é falha de coleta engolida (caso CAGED 2025-03: FTP devolveu
#: 550 transitório, o WARNING passou e o painel saiu sem o mês — distorcendo
#: o bimestre 25b2 em ~metade sem nenhum aviso).
LACUNAS_DOCUMENTADAS = {
    # SIDRA 6579 não publicou estimativas municipais para 2022-2023 (Censo).
    "populacao_ibge_sidra_6579": {(2022, m) for m in range(1, 13)}
    | {(2023, m) for m in range(1, 13)},
    # STN: conteúdo dos meses nunca publicado no CKAN (arquivo AAAAMM traz
    # conteúdo AAAAMM-1 de ~set a ~nov e re-sincroniza em dez).
    "transferencias_municipais_stn": {
        (a, m) for a, m in LACUNAS_UPSTREAM_POR_FONTE["transferencias_municipais_stn"]
    },
}


def _meses_interiores_faltantes(meses_presentes: set[tuple[int, int]]):
    """(ano, mes) ausentes entre o primeiro e o último mês presentes."""
    (ano_i, mes_i), (ano_f, mes_f) = min(meses_presentes), max(meses_presentes)
    faltantes = []
    ano, mes = ano_i, mes_i
    while (ano, mes) <= (ano_f, mes_f):
        if (ano, mes) not in meses_presentes:
            faltantes.append((ano, mes))
        mes += 1
        if mes == 13:
            ano, mes = ano + 1, 1
    return faltantes


def test_continuidade_mensal_por_fonte(painel_mensal):
    """Para cada fonte da matriz, os meses com dado no painel mensal formam
    uma série CONTÍNUA (sem buraco interior), exceto as lacunas documentadas.

    Bordas não contam (cobertura real da fonte: BPC desde 2019, SIOF só 2026,
    CAGED até o último mês publicado). Um buraco interior novo = coleta que
    falhou silenciosamente → este teste fica vermelho em vez de o painel sair
    com meio-bimestre disfarçado de valor válido.
    """
    regras = carregar_matriz_regras()
    problemas = {}
    for fonte, grupo in regras.groupby("fonte"):
        cols = [v for v in grupo["variavel"] if v in painel_mensal.columns]
        com_dado = painel_mensal[painel_mensal[cols].notna().any(axis=1)]
        if com_dado.empty:
            continue
        presentes = {
            (int(a), int(m))
            for a, m in com_dado[["ano", "mes"]].drop_duplicates().itertuples(index=False)
        }
        faltantes = set(_meses_interiores_faltantes(presentes))
        nao_documentados = faltantes - LACUNAS_DOCUMENTADAS.get(fonte, set())
        if nao_documentados:
            problemas[fonte] = sorted(nao_documentados)
    assert not problemas, (
        "Buraco de mês NÃO documentado no interior da série (falha de coleta "
        f"engolida — caso CAGED 2025-03): {problemas}"
    )


def test_caged_2025_03_presente(painel_mensal):
    """Regressão do incidente de 2026-06-10: 2025-03 tem CAGED nas 14 regiões
    (a primeira recoleta perdeu o mês por 550 transitório do FTP e o bimestre
    25b2 saiu com fluxos pela metade)."""
    m = painel_mensal[(painel_mensal["ano"] == 2025) & (painel_mensal["mes"] == 3)]
    assert len(m) == 14
    for col in ["admissoes", "desligamentos", "saldo", "total_movimentacoes"]:
        assert m[col].notna().all(), f"2025-03 sem {col} — mês perdido de novo"


def test_bimestre_25b2_ordem_de_grandeza(painel_bim):
    """25b2 (mar-abr/2025) com os 2 meses: fluxos na ordem dos vizinhos.

    Com o mês 2025-03 perdido, as admissões estaduais de 25b2 caíam para
    ~56 mil contra 112-121 mil dos vizinhos (erro de ~2x — invisível para o
    gate de outliers, calibrado para erros de unidade ~100x)."""
    adm = painel_bim.groupby(["ano", "bimestre"])["admissoes"].sum()
    razao_b1 = adm.loc[(2025, 2)] / adm.loc[(2025, 1)]
    razao_b3 = adm.loc[(2025, 2)] / adm.loc[(2025, 3)]
    assert 0.7 < razao_b1 < 1.4
    assert 0.7 < razao_b3 < 1.4


def test_bimestres_lacuna_upstream_anulados(painel_bim):
    """Bimestres com mês não publicado pela STN ficam NaN em TODA a família
    transf_fed_* (meio-bimestre não pode passar por valor válido); as demais
    fontes do mesmo bimestre ficam intactas."""
    regras = carregar_matriz_regras()
    vars_stn = [
        v
        for v in regras.loc[
            regras["fonte"] == "transferencias_municipais_stn", "variavel"
        ]
        if v in painel_bim.columns
    ]
    assert vars_stn
    for ano, mes in LACUNAS_UPSTREAM_POR_FONTE["transferencias_municipais_stn"]:
        bimestre = (mes + 1) // 2
        sub = painel_bim[
            (painel_bim["ano"] == ano) & (painel_bim["bimestre"] == bimestre)
        ]
        assert len(sub) == 14
        for v in vars_stn:
            assert sub[v].isna().all(), f"{ano}b{bimestre} {v} deveria ser NaN"
        assert sub["bf_valor_total"].notna().all(), "outras fontes não podem ser afetadas"


def test_meses_stn_backfillados_presentes(painel_mensal):
    """2018-05 (reconstruído por inferência item+ordem: arquivo sem coluna
    destino) e 2021-08 (arquivo corrigido pela fonte) entram no painel."""
    for ano, mes in [(2018, 5), (2021, 8)]:
        m = painel_mensal[(painel_mensal["ano"] == ano) & (painel_mensal["mes"] == mes)]
        assert m["transf_fed_total"].notna().any(), f"{ano}-{mes:02d} sem transf_fed"
        # ordem de grandeza estadual coerente (sem erro de unidade da inferência)
        total = m["transf_fed_total"].sum()
        vizinhos = painel_mensal[
            (painel_mensal["ano"] == ano)
            & (painel_mensal["mes"].isin([mes - 1, mes + 1]))
        ]["transf_fed_total"].sum() / 2
        assert 0.2 < total / vizinhos < 5

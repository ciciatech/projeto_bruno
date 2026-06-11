"""Testes do parser ESTBAN (sem rede, fixture sintética no layout wide do BCB).

O layout real (site moderno, 06/2026) tem 2 linhas de preâmbulo, header
"#DATA_BASE;UF;...;VERBETE_NNN_*;CODMUN_IBGE" com verbetes em COLUNAS,
encoding latin-1 e separador ';'. Regressões cobertas:

1. Sniff do preâmbulo (skiprows dinâmico, não fixo).
2. WIDE→LONG via ``_melt_wide`` (colunas VERBETE_NNN_* → verbete/saldo).
3. Filtro BNB: o nome real é "BCO DO NORDESTE DO BRASIL S.A." — o filtro
   antigo só casava "BANCO DO NORDESTE" e perdia tudo sem o fallback de CNPJ.
4. Verbete 160 é o TOTAL de operações de crédito; somar 160+161+... contaria
   em dobro (bug do VERBETES_CREDITO antigo).
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from pipeline.extract.estban import (
    _agregar_municipal,
    _filtrar_ce_bnb,
    _ler_arquivo,
    _melt_wide,
    _montar_painel,
    _normalizar,
    _prefiltrar_uf,
)

FIXTURE = Path(__file__).parent / "data" / "estban_wide_sintetico.csv"


def _pipeline_ate_filtro(path: Path) -> pd.DataFrame:
    """Encadeia leitura → prefiltro UF → melt → normalização → filtro CE+BNB."""
    raw = _ler_arquivo(path)
    return _filtrar_ce_bnb(_normalizar(_melt_wide(_prefiltrar_uf(raw, "CE"))))


def test_ler_arquivo_sniffa_preambulo_do_layout_wide():
    """As 2 linhas de preâmbulo do BCB não podem virar header/dados."""
    raw = _ler_arquivo(FIXTURE)

    assert "#DATA_BASE" in raw.columns
    assert "CODMUN_IBGE" in raw.columns
    assert "VERBETE_160_OPERACOES_DE_CREDITO" in raw.columns
    assert len(raw) == 4  # só as linhas de dados


def test_ler_arquivo_zip_le_csv_interno(tmp_path: Path):
    """Os meses do BCB chegam zipados (.ZIP até 2022, .csv.zip de 2023 em
    diante) — pandas deve ler o CSV interno sem extração manual."""
    alvo = tmp_path / "202401_ESTBAN.csv.zip"
    with zipfile.ZipFile(alvo, "w") as zf:
        zf.write(FIXTURE, "202401_ESTBAN.CSV")

    raw = _ler_arquivo(alvo)

    assert len(raw) == 4
    assert "VERBETE_160_OPERACOES_DE_CREDITO" in raw.columns


def test_melt_wide_derrete_verbetes_para_formato_longo():
    raw = _ler_arquivo(FIXTURE)

    longo = _melt_wide(raw)

    assert "CD_VERBETE_ESTBAN" in longo.columns
    assert "SALDO" in longo.columns
    # 4 linhas × 3 colunas de verbete = 12 linhas longas
    assert len(longo) == 12
    assert set(longo["CD_VERBETE_ESTBAN"]) == {"160", "161", "162"}
    # colunas de identificação preservadas
    assert {"#DATA_BASE", "UF", "CNPJ", "NOME_INSTITUICAO", "CODMUN_IBGE"} <= set(
        longo.columns
    )


def test_melt_wide_passa_formato_longo_intacto():
    """DataFrames já longos (Base dos Dados, mirrors) não devem ser alterados."""
    df = pd.DataFrame(
        {"id_municipio": ["2303709"], "id_verbete": ["160"], "valor": ["10"]}
    )
    assert _melt_wide(df) is df


def test_filtro_bnb_casa_grafia_real_bco_do_nordeste():
    """Regressão: 'BCO DO NORDESTE DO BRASIL S.A.' (grafia real) tem que casar
    pelo NOME mesmo quando o CNPJ não é o do BNB (linha Barbalha, CNPJ
    99999999). Linhas de outros bancos e de outras UFs ficam fora."""
    filt = _pipeline_ate_filtro(FIXTURE)

    assert set(filt["_ibge7"]) == {"2303709", "2301901"}  # Caucaia + Barbalha
    assert "3550308" not in set(filt["_ibge7"])  # São Paulo (BNB, mas fora do CE)
    # BCO XYZ (não-BNB) excluído: nenhum saldo 77777777 sobrevive
    assert not (filt["valor"] == 77777777).any()


def test_verbete_160_e_total_nao_soma_dobrado():
    """O 160 já é o saldo TOTAL de operações de crédito; o total do painel deve
    ser o 160 puro, NÃO 160+161+162 (dupla contagem do código antigo)."""
    cons = _agregar_municipal(_pipeline_ate_filtro(FIXTURE))

    caucaia = cons[cons["cod_ibge"] == "2303709"].iloc[0]
    assert caucaia["credito_bnb_total"] == 51417436  # e não 51417436+30M+21,4M
    assert caucaia["bnb_emprestimos_titulos_descontados"] == 30000000
    assert caucaia["bnb_financiamentos"] == 21417436
    assert int(caucaia["ano"]) == 2024
    assert int(caucaia["mes"]) == 1


def test_painel_mapeia_regioes_ce():
    """Saída do painel: cod_ibge, regiao_codigo, regiao_nome, ano, mes,
    credito_operacoes — com as regiões de planejamento SEPLAG/IPECE."""
    painel = _montar_painel(_agregar_municipal(_pipeline_ate_filtro(FIXTURE)))

    assert list(painel.columns) == [
        "cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes", "credito_operacoes",
    ]
    porlinha = painel.set_index("cod_ibge")
    assert porlinha.loc["2303709", "regiao_nome"] == "Grande Fortaleza"
    assert porlinha.loc["2301901", "regiao_nome"] == "Cariri"
    assert porlinha.loc["2303709", "credito_operacoes"] == 51417436
    assert porlinha.loc["2301901", "credito_operacoes"] == 1000000


def test_fonte_longa_sem_verbete_160_usa_soma_das_aberturas():
    """Fontes longas antigas podem não trazer o agregado 160; nesse caso o
    fallback soma as aberturas (161, 162, ...) em vez de zerar o total."""
    df = pd.DataFrame(
        {
            "_ibge7": ["2303709", "2303709"],
            "ano": [2024, 2024],
            "mes": [1, 1],
            "verbete": ["161", "162"],
            "valor": [600.0, 400.0],
        }
    )

    cons = _agregar_municipal(df)

    assert cons["credito_bnb_total"].iloc[0] == 1000.0


def test_codmun_ibge_nulo_nao_explode(tmp_path: Path):
    """Regressão: 202209_ESTBAN.ZIP real tem 3 linhas com CODMUN_IBGE vazio
    (ex.: BTG Pactual/Fortaleza) — viravam NaN e quebravam o ``len(x)`` do
    filtro CE, derrubando o MÊS INTEIRO. Devem ser apenas descartadas."""
    texto = FIXTURE.read_text(encoding="latin-1")
    texto += "202401;CE;9999;FORTALEZA;30306294;BANCO BTG PACTUAL S.A.;1;1;5;1;2;\n"
    alvo = tmp_path / "202401_ESTBAN.CSV"
    alvo.write_bytes(texto.encode("latin-1"))

    filt = _pipeline_ate_filtro(alvo)

    # BNB CE preservado; linha com código nulo (não-BNB) fora, sem exceção
    assert set(filt["_ibge7"]) == {"2303709", "2301901"}


def test_agregar_municipal_vazio_retorna_vazio():
    assert _agregar_municipal(pd.DataFrame()).empty
    assert _montar_painel(pd.DataFrame()).empty

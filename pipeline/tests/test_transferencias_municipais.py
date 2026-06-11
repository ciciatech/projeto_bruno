"""Testes de regressão do parser de transferências constitucionais STN (T32).

Cobre o bug dos valores em centavos: alguns CSVs do CKAN do Tesouro (ex.: o
arquivo com os dados de set/2024) publicam os decêndios como inteiros em
centavos ("70201795" = R$ 702.017,95) em vez de decimais, inflando os valores
em exatamente 100x. O fix detecta o formato a nível de arquivo (<1% dos
valores brutos com separador decimal) e divide por 100 — apenas nesse caso.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from pipeline.extract.transferencias_municipais import (
    _detectar_centavos,
    _processar_csv_ce,
)

_HEADER = (
    "Município;UF;ANO;Mês;1º Decêndio;2º Decêndio;3º Decêndio;"
    "Item transferência;Transferência"
)


def _csv_sintetico(tmp_path: Path, nome: str, linhas: list[str]) -> Path:
    """Escreve um CSV no formato STN (sep ';', latin-1)."""
    path = tmp_path / nome
    path.write_text("\n".join([_HEADER] + linhas) + "\n", encoding="latin-1")
    return path


def _aprox(v: float):
    return pytest.approx(v, abs=0.01)


def test_formato_decimal_ponto_nao_divide(tmp_path):
    """Arquivo com decimais ponto (ex.: ago/2024) deve ser lido tal qual."""
    path = _csv_sintetico(
        tmp_path,
        "decimal.csv",
        [
            "FORTALEZA;CE;2024;08;643778.42;100.00;0.58;FPM;FPM",
            "SOBRAL;CE;2024;08;1000.50;2000.25;3000.25;FPM;FPM",
            "NATAL;RN;2024;08;111.11;222.22;333.33;FPM;FPM",
        ],
    )
    out = _processar_csv_ce(path)

    fort = out[out["_chave"].str.contains("fortaleza")]
    assert fort["valor"].iloc[0] == _aprox(643879.00)
    sobral = out[out["_chave"].str.contains("sobral")]
    assert sobral["valor"].iloc[0] == _aprox(6001.00)


def test_formato_decimal_virgula_nao_divide(tmp_path):
    """Formato antigo vírgula-decimal também não pode ser dividido."""
    path = _csv_sintetico(
        tmp_path,
        "virgula.csv",
        ["FORTALEZA;CE;2020;05;643778,42;0,00;0,00;FPM;FPM"],
    )
    out = _processar_csv_ce(path)

    assert out["valor"].iloc[0] == _aprox(643778.42)


def test_formato_inteiro_centavos_divide_por_100(tmp_path):
    """Regressão T32: arquivo inteiro-centavos (set/2024) deve sair em reais."""
    path = _csv_sintetico(
        tmp_path,
        "centavos.csv",
        [
            "FORTALEZA;CE;2024;09;70201795;0;0;FPM;FPM",
            "SOBRAL;CE;2024;09;100000;200000;300000;FPM;FPM",
            "NATAL;RN;2024;09;12345678;0;0;FPM;FPM",
        ],
    )
    out = _processar_csv_ce(path)

    fort = out[out["_chave"].str.contains("fortaleza")]
    assert fort["valor"].iloc[0] == _aprox(702017.95)
    sobral = out[out["_chave"].str.contains("sobral")]
    assert sobral["valor"].iloc[0] == _aprox(6000.00)


def test_detectar_centavos_ignora_zeros_isolados():
    """Poucos inteiros (ex.: '0') num arquivo majoritariamente decimal não
    podem disparar a divisão — o limiar é <1% de valores com separador."""
    df = pd.DataFrame(
        {
            "dec1": ["100.00"] * 99 + ["0"],
            "dec2": ["200.50"] * 100,
            "dec3": ["0.00"] * 100,
        }
    )
    assert _detectar_centavos(df) is False


def test_detectar_centavos_arquivo_todo_inteiro():
    df = pd.DataFrame(
        {"dec1": ["70201795", "100"], "dec2": ["0", "0"], "dec3": ["50", "1"]}
    )
    assert _detectar_centavos(df) is True


# ---------------------------------------------------------------------------
# Layout sem coluna "Transferência" (caso 2018-05): inferência item + ordem
# ---------------------------------------------------------------------------

_HEADER_SEM_DESTINO = (
    "Município;UF;ANO;Mês;1º Decêndio;2º Decêndio;3º Decêndio;"
    "Item transferência;"
)


def _csv_sem_destino(tmp_path: Path, linhas: list[str]) -> Path:
    path = tmp_path / "sem_destino.csv"
    path.write_text(
        "\n".join([_HEADER_SEM_DESTINO] + linhas) + "\n", encoding="latin-1"
    )
    return path


def test_layout_sem_destino_infere_fpm_e_fundeb(tmp_path):
    """Arquivo no layout do 201805 (header da última coluna vazio): 1ª linha
    de FPM = destino FPM, 2ª = retenção FUNDEB; FPE/ICMS → FUNDEB; FEP →
    Royalties; Lei Kandir → outros."""
    path = _csv_sem_destino(
        tmp_path,
        [
            "FORTALEZA;CE;2018;05;800.00;0;0;FPM;",      # 1ª FPM → fpm_dest
            "FORTALEZA;CE;2018;05;0;0;376.76;ICMS/LC 87/96 - Lei Kandir;",
            "FORTALEZA;CE;2018;05;200.00;0;0;FPM;",      # 2ª FPM → fundeb_dest
            "FORTALEZA;CE;2018;05;50.00;0;0;FPE;",       # → fundeb_dest
            "FORTALEZA;CE;2018;05;10.00;0;0;FEP;",       # → royalties_dest
        ],
    )
    out = _processar_csv_ce(path)
    por = out.set_index("dest_canon")["valor"]
    assert por["fpm_dest"] == _aprox(800.00)
    assert por["fundeb_dest"] == _aprox(250.00)
    assert por["royalties_dest"] == _aprox(10.00)
    assert por["outros_dest"] == _aprox(376.76)


def test_layout_sem_destino_itr_linha_unica_e_fundeb(tmp_path):
    """ITR com linha única no município é a retenção FUNDEB (todos os
    municípios têm a linha FUNDEB; nem todos a do destino ITR); com duas
    linhas, a 1ª é o destino ITR."""
    path = _csv_sem_destino(
        tmp_path,
        [
            "FORTALEZA;CE;2018;05;30.00;0;0;ITR;",   # única → fundeb
            "SOBRAL;CE;2018;05;40.00;0;0;ITR;",      # 1ª de duas → itr
            "SOBRAL;CE;2018;05;8.00;0;0;ITR;",       # 2ª → fundeb
        ],
    )
    out = _processar_csv_ce(path)
    fort = out[out["_chave"].str.contains("fortaleza")].set_index("dest_canon")["valor"]
    sobral = out[out["_chave"].str.contains("sobral")].set_index("dest_canon")["valor"]
    assert "itr_dest" not in fort.index
    assert fort["fundeb_dest"] == _aprox(30.00)
    assert sobral["itr_dest"] == _aprox(40.00)
    assert sobral["fundeb_dest"] == _aprox(8.00)


def test_layout_sem_destino_item_desconhecido_aborta(tmp_path):
    """Item fora do universo validado (2018-2021) NÃO pode ser aproximado em
    silêncio — os itens pós-2021 (ex.: COUN VAAF) mudaram a decomposição.
    O parse aborta e o guard de continuidade acusa o mês."""
    path = _csv_sem_destino(
        tmp_path,
        [
            "FORTALEZA;CE;2023;11;100.00;0;0;FPM;",
            "FORTALEZA;CE;2023;11;50.00;0;0;COUN VAAF;",
        ],
    )
    with pytest.raises(ValueError, match="COUN VAAF"):
        _processar_csv_ce(path)


def test_layout_com_destino_nao_dispara_inferencia(tmp_path):
    """Arquivo bem formado segue o caminho normal (sem inferência) mesmo que
    a regra de ocorrência discordasse do rótulo explícito."""
    path = _csv_sintetico(
        tmp_path,
        "ok.csv",
        ["FORTALEZA;CE;2024;08;100.00;0;0;FPM;FUNDEB"],  # rótulo manda
    )
    out = _processar_csv_ce(path)
    assert out.set_index("dest_canon")["valor"]["fundeb_dest"] == _aprox(100.00)

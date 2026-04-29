"""
Mapeamento município → região de planejamento do Ceará (14 macrorregiões SEPLAG/IPECE).

Fonte da divisão regional: IPECE Texto para Discussão nº 111 (2015) e SIOF-CE.
Os 184 municípios cearenses estão em ``pipeline/data/municipios_ce_regioes.csv``.
Os códigos IBGE de 7 dígitos vêm da API ``servicodados.ibge.gov.br`` (sem auth)
e são cacheados em ``pipeline/data/municipios_ce_ibge_cache.csv`` para uso offline.

Os códigos das 14 regiões são os mesmos usados pelo SIOF-CE (``pipeline/extract/siof.py``).
"""

from __future__ import annotations

import logging
import unicodedata
from functools import lru_cache
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
MUNICIPIOS_REGIOES_CSV = DATA_DIR / "municipios_ce_regioes.csv"
IBGE_CACHE_CSV = DATA_DIR / "municipios_ce_ibge_cache.csv"
IBGE_API_URL = (
    "https://servicodados.ibge.gov.br/api/v1/localidades/estados/23/municipios"
)

REGIOES_CE = {
    "01": "Cariri",
    "02": "Centro Sul",
    "03": "Grande Fortaleza",
    "04": "Litoral Leste",
    "05": "Litoral Norte",
    "06": "Litoral Oeste / Vale do Curu",
    "07": "Maciço do Baturité",
    "08": "Serra da Ibiapaba",
    "09": "Sertão Central",
    "10": "Sertão de Canindé",
    "11": "Sertão de Sobral",
    "12": "Sertão dos Crateús",
    "13": "Sertão dos Inhamuns",
    "14": "Vale do Jaguaribe",
}


# Aliases para nomes históricos / variantes de grafia que aparecem em bases públicas
# (Portal da Transparência, ESTBAN antigo, etc). A chave é o nome após normalização
# básica; o valor é o nome oficial atual do IBGE (também já normalizado).
_ALIASES_NOME = {
    "itapage": "itapaje",  # mudança ortográfica 2018 (Lei CE 16.550): Itapagé → Itapajé
}


def _normalizar_nome(nome: str) -> str:
    """Normaliza nome de município para merge: lowercase, sem acentos, sem espaços extras."""
    if not isinstance(nome, str):
        return ""
    s = unicodedata.normalize("NFKD", nome)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    return _ALIASES_NOME.get(s, s)


def _baixar_municipios_ibge() -> pd.DataFrame:
    """Busca lista de municípios do CE via API IBGE. Retorna (cod_ibge, nome)."""
    logger.info("Baixando lista de municípios CE da API IBGE...")
    resp = requests.get(IBGE_API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame([{"cod_ibge": str(m["id"]), "nome_ibge": m["nome"]} for m in data])
    return df


def _carregar_municipios_ibge() -> pd.DataFrame:
    """Carrega cache local de municípios IBGE; baixa da API se não existir."""
    if IBGE_CACHE_CSV.exists():
        return pd.read_csv(IBGE_CACHE_CSV, dtype={"cod_ibge": str})
    df = _baixar_municipios_ibge()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(IBGE_CACHE_CSV, index=False)
    logger.info(f"Cache IBGE salvo: {IBGE_CACHE_CSV} ({len(df)} municípios)")
    return df


@lru_cache(maxsize=1)
def get_regiao_info() -> pd.DataFrame:
    """
    Retorna DataFrame com (cod_ibge, municipio_nome, regiao_codigo, regiao_nome)
    para os 184 municípios cearenses, mesclado com códigos IBGE oficiais.
    """
    base = pd.read_csv(MUNICIPIOS_REGIOES_CSV, dtype={"regiao_codigo": str})
    base["regiao_codigo"] = base["regiao_codigo"].str.zfill(2)
    base["_chave"] = base["municipio_nome"].apply(_normalizar_nome)

    ibge = _carregar_municipios_ibge()
    ibge["_chave"] = ibge["nome_ibge"].apply(_normalizar_nome)

    merged = base.merge(ibge, on="_chave", how="left")
    faltantes = merged[merged["cod_ibge"].isna()]
    if len(faltantes) > 0:
        nomes = faltantes["municipio_nome"].tolist()
        raise ValueError(
            f"{len(faltantes)} municípios CE não casaram com a lista IBGE: {nomes}. "
            "Verifique grafia em pipeline/data/municipios_ce_regioes.csv."
        )

    if len(merged) != 184:
        raise ValueError(f"Esperados 184 municípios CE, encontrados {len(merged)}.")
    if merged["regiao_codigo"].nunique() != 14:
        raise ValueError(
            f"Esperadas 14 regiões, encontradas {merged['regiao_codigo'].nunique()}."
        )

    out = merged[["cod_ibge", "municipio_nome", "nome_ibge", "regiao_codigo"]].copy()
    out["regiao_nome"] = out["regiao_codigo"].map(REGIOES_CE)
    return out.sort_values(["regiao_codigo", "municipio_nome"]).reset_index(drop=True)


def get_mapping_cod_ibge_para_regiao() -> dict[str, str]:
    """Dict {cod_ibge_7dig: regiao_codigo} para os 184 municípios CE."""
    df = get_regiao_info()
    return dict(zip(df["cod_ibge"], df["regiao_codigo"]))


def get_codigos_municipios_ce() -> list[str]:
    """Lista dos 184 códigos IBGE de 7 dígitos dos municípios cearenses."""
    return get_regiao_info()["cod_ibge"].tolist()


def agregar_para_regiao(
    df: pd.DataFrame,
    col_cod_ibge: str,
    cols_valor: list[str] | str,
    cols_groupby_extra: list[str] | None = None,
) -> pd.DataFrame:
    """
    Agrega um DataFrame municipal para nível regional, somando ``cols_valor`` por
    ``regiao_codigo`` (e ``cols_groupby_extra``, ex: ano/mês).

    O DataFrame de entrada precisa ter ``col_cod_ibge`` com códigos IBGE de 7 dígitos.
    Linhas com município fora do CE são descartadas.
    """
    if isinstance(cols_valor, str):
        cols_valor = [cols_valor]
    cols_groupby_extra = cols_groupby_extra or []

    info = get_regiao_info()[["cod_ibge", "regiao_codigo", "regiao_nome"]]

    work = df.copy()
    work[col_cod_ibge] = work[col_cod_ibge].astype(str).str.zfill(7)
    work = work.merge(info, left_on=col_cod_ibge, right_on="cod_ibge", how="inner")

    grupo = ["regiao_codigo", "regiao_nome", *cols_groupby_extra]
    agg = work.groupby(grupo, as_index=False)[cols_valor].sum(min_count=1)
    return agg.sort_values(grupo).reset_index(drop=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    info = get_regiao_info()
    print(f"\n{len(info)} municípios CE × {info['regiao_codigo'].nunique()} regiões\n")
    print(info.groupby(["regiao_codigo", "regiao_nome"]).size().rename("n_municipios"))

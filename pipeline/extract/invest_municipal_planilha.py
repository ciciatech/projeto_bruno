"""
Adaptador para a planilha de investimento municipal do Bruno.

O Prof. Paulo informou que Bruno já tem os dados de investimento municipal
agregados em planilha local. Esse adaptador lê o arquivo (xlsx ou csv) e
normaliza para o painel regional CE.

## Onde colocar o arquivo

Salve o arquivo do Bruno em ``dados_nordeste/raw/invest_municipal/`` com
qualquer nome (.xlsx, .xls ou .csv). O adaptador lê todos os arquivos no
diretório.

## Schema esperado (mínimo)

Aceita as seguintes formas de identificação do município (na ordem de
preferência, primeira encontrada vence):

- coluna ``codigo_ibge`` (ou ``cod_ibge``, ``id_municipio``) — preferível: 7 dígitos
- coluna ``municipio`` (nome) — fallback, casamos por nome normalizado

Para o período, aceitamos:

- ``ano`` + ``mes`` (separados)
- ``data`` no formato AAAA-MM ou AAAAMM
- ``periodo`` no formato AAAA-MM
- ``ano`` apenas → vira valor anual; o painel mensal preencherá com mes=NaN
  e o consolidador agregador pode tratar como anual.

Para o valor:

- ``valor`` (R$) — esperado nominal. Se o Bruno tiver coluna ``valor_real`` ou
  ``valor_deflacionado``, usamos ela preferencialmente.

Saída: ``dados_nordeste/processed/invest_municipal/invest_municipal_ce.csv``
com (cod_ibge, regiao_codigo, regiao_nome, ano, mes, valor).
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

from pipeline.config import RAW_DIR
from pipeline.extract._portal_transp_csv import _info_municipios_ce_indexado
from pipeline.regioes_ce import _normalizar_nome
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)


def _slug(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _detectar(headers: list[str]) -> dict[str, str]:
    canon: dict[str, str] = {}
    for col in headers:
        s = _slug(col)
        if s in {"codigo_ibge", "cod_ibge", "id_municipio", "ibge"}:
            canon["cod_ibge"] = col
        elif s in {"municipio", "nome_municipio", "municipio_nome", "cidade"}:
            canon["municipio"] = col
        elif s == "ano":
            canon["ano"] = col
        elif s in {"mes", "mes_competencia"}:
            canon["mes"] = col
        elif s in {"data", "periodo", "data_base", "ano_mes"}:
            canon["data"] = col
        elif s in {"valor_real", "valor_deflacionado"}:
            canon["valor"] = col
        elif s in {"valor", "valor_nominal", "valor_pago", "valor_investido"} and "valor" not in canon:
            canon["valor"] = col
    return canon


def _ler_arquivo(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)
    if suf == ".csv":
        for sep in [";", ","]:
            for enc in ["utf-8", "latin-1", "utf-8-sig"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str)
                    if df.shape[1] > 1:
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
    raise ValueError(f"Não foi possível ler {path}")


def _normalizar(df: pd.DataFrame) -> pd.DataFrame:
    canon = _detectar(list(df.columns))
    if "valor" not in canon:
        raise ValueError(
            f"Coluna de valor não encontrada. Headers: {list(df.columns)}. "
            "Esperado: 'valor' ou 'valor_real'."
        )
    if not (("cod_ibge" in canon) or ("municipio" in canon)):
        raise ValueError(
            "Necessário 'codigo_ibge' ou 'municipio' (nome) para identificar municípios."
        )

    out = pd.DataFrame()
    if "cod_ibge" in canon:
        out["cod_ibge"] = (
            df[canon["cod_ibge"]].astype(str).str.replace(r"\D", "", regex=True).str.zfill(7)
        )
    if "municipio" in canon:
        out["_chave"] = df[canon["municipio"]].apply(_normalizar_nome)

    if "data" in canon:
        s = df[canon["data"]].astype(str).str.replace(r"\D", "", regex=True)
        out["ano"] = pd.to_numeric(s.str[:4], errors="coerce").astype("Int64")
        out["mes"] = pd.to_numeric(s.str[4:6], errors="coerce").astype("Int64")
    else:
        out["ano"] = pd.to_numeric(df.get(canon.get("ano")), errors="coerce").astype("Int64")
        if "mes" in canon:
            out["mes"] = pd.to_numeric(df[canon["mes"]], errors="coerce").astype("Int64")
        else:
            out["mes"] = pd.NA  # dado anual sem mês

    out["valor"] = pd.to_numeric(
        df[canon["valor"]].astype(str).str.replace(".", "", regex=False).str.replace(",", "."),
        errors="coerce",
    ).fillna(0)

    return out


def coletar_de_diretorio(diretorio: Path | str | None = None) -> pd.DataFrame:
    """Lê todos os arquivos em ``dados_nordeste/raw/invest_municipal/`` e gera consolidado."""
    diretorio = Path(diretorio) if diretorio else (RAW_DIR / "invest_municipal")
    if not diretorio.exists():
        logger.warning(
            f"Diretório {diretorio} não existe. Coloque a planilha do Bruno lá "
            "(formato xlsx/xls/csv com colunas município, ano, mês e valor)."
        )
        return pd.DataFrame()

    arquivos = [
        p for p in diretorio.rglob("*")
        if p.is_file() and p.suffix.lower() in {".csv", ".xlsx", ".xls"}
    ]
    if not arquivos:
        logger.warning(f"Invest. municipal: nenhum arquivo em {diretorio}.")
        return pd.DataFrame()

    frames = []
    for arq in arquivos:
        try:
            raw = _ler_arquivo(arq)
            norm = _normalizar(raw)
            frames.append(norm)
            logger.info(f"  {arq.name}: {len(raw)} linhas")
        except Exception as e:
            logger.error(f"  {arq.name}: erro — {e}")

    if not frames:
        return pd.DataFrame()

    bruto = pd.concat(frames, ignore_index=True)

    info = _info_municipios_ce_indexado()
    if "cod_ibge" not in bruto.columns:
        bruto = bruto.merge(info, on="_chave", how="left")
    elif "regiao_codigo" not in bruto.columns:
        bruto = bruto.merge(
            info[["cod_ibge", "regiao_codigo", "regiao_nome"]], on="cod_ibge", how="left"
        )
    bruto = bruto.dropna(subset=["cod_ibge"]).copy()

    cols_grp = ["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes"]
    out = (
        bruto.groupby(cols_grp, dropna=False, as_index=False)["valor"]
        .sum()
        .sort_values(["ano", "mes", "cod_ibge"])
        .reset_index(drop=True)
    )

    save_dataframe(
        out,
        "invest_municipal_ce",
        subdir="processed",
        path_parts=["invest_municipal"],
    )
    logger.info(
        f"Invest. municipal: {len(out)} linhas, {out['cod_ibge'].nunique()} municípios CE"
    )
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_de_diretorio()

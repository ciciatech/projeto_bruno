"""
Coletor de transferências estaduais cota-parte (ICMS, IPVA, FPE-derivados) para
municípios cearenses.

## Por que adaptador

A SEFAZ-CE publica esses dados via Ceará Transparente
(https://cearatransparente.ce.gov.br/portal-da-transparencia/despesas/transferencias-a-municipios)
mas o endpoint do portal está atrás de WAF agressivo que bloqueia requests
programáticos sem sessão de browser. Para evitar acoplar a pipeline a um
scraper headless frágil, este coletor funciona como **adaptador**: lê arquivos
Excel/CSV que o pesquisador baixa manualmente do Ceará Transparente (um por
ano, ou um histórico consolidado) e os normaliza para o painel regional.

## Onde baixar

Acesse https://cearatransparente.ce.gov.br/portal-da-transparencia/despesas/transferencias-a-municipios
no navegador, exporte o relatório por município/mês para os anos desejados, e
salve em ``dados_nordeste/raw/sefaz_ce/`` (qualquer subdiretório).

## Schema esperado

Aceita CSV ou XLSX com pelo menos as colunas (case/acento-insensitive):
- ``municipio`` (nome) ou ``codigo_ibge``
- ``ano``, ``mes`` (ou ``data``/``periodo``)
- ``tipo_transferencia`` (ex: ICMS, IPVA, FUNDEB, FPE) ou colunas separadas por tipo
- ``valor`` (em R$)

Saída: ``dados_nordeste/processed/sefaz_ce/transf_estaduais_ce_mensal.csv``
com (cod_ibge, regiao_codigo, regiao_nome, ano, mes, icms_dest, ipva_dest,
outros_dest, total).
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


_TIPOS = {
    "ICMS": "icms_dest",
    "IPVA": "ipva_dest",
    "FPE": "fpe_dest",
    "FUNDEB": "fundeb_estado_dest",
    "ROYALTIES": "royalties_estado_dest",
    "ICMS DESONERAC": "icms_dest",
}


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
        if s in {"municipio", "nome_municipio", "municipio_nome"}:
            canon["municipio"] = col
        elif s in {"codigo_ibge", "cod_ibge", "id_municipio"}:
            canon["cod_ibge"] = col
        elif s == "ano":
            canon["ano"] = col
        elif s in {"mes", "mes_competencia"}:
            canon["mes"] = col
        elif s in {"data", "periodo", "data_base"}:
            canon["data"] = col
        elif s in {"tipo", "tipo_transferencia", "transferencia"}:
            canon["tipo"] = col
        elif s in {"valor", "valor_pago", "valor_repassado"}:
            canon["valor"] = col
    return canon


def _ler_arquivo(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)
    if suf == ".csv":
        for sep in [";", ","]:
            for enc in ["latin-1", "utf-8", "utf-8-sig"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str)
                    if df.shape[1] > 1:
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
    raise ValueError(f"Não foi possível ler {path}")


def _normalizar(df: pd.DataFrame) -> pd.DataFrame:
    canon = _detectar(list(df.columns))
    obrig = ["valor"]
    if not (("municipio" in canon) or ("cod_ibge" in canon)):
        raise ValueError(
            f"Necessário 'municipio' ou 'codigo_ibge'. Detectado: {list(canon.keys())}"
        )
    falt = [c for c in obrig if c not in canon]
    if falt:
        raise ValueError(f"Colunas obrigatórias ausentes: {falt}")

    out = pd.DataFrame()
    if "municipio" in canon:
        out["_chave"] = df[canon["municipio"]].apply(_normalizar_nome)
    if "cod_ibge" in canon:
        out["cod_ibge"] = df[canon["cod_ibge"]].astype(str).str.replace(r"\D", "", regex=True)

    if "data" in canon:
        s = df[canon["data"]].astype(str).str.replace(r"\D", "", regex=True)
        out["ano"] = pd.to_numeric(s.str[:4], errors="coerce").astype("Int64")
        out["mes"] = pd.to_numeric(s.str[4:6], errors="coerce").astype("Int64")
    else:
        out["ano"] = pd.to_numeric(df.get(canon.get("ano")), errors="coerce").astype("Int64")
        out["mes"] = pd.to_numeric(df.get(canon.get("mes")), errors="coerce").astype("Int64")

    out["tipo"] = df.get(canon.get("tipo"), "DESCONHECIDO").astype(str).str.upper().str.strip()
    out["valor"] = pd.to_numeric(
        df[canon["valor"]].astype(str).str.replace(".", "", regex=False).str.replace(",", "."),
        errors="coerce",
    ).fillna(0)
    out["tipo_canon"] = out["tipo"].map(
        lambda t: next((v for k, v in _TIPOS.items() if k in t), "outros_dest")
    )
    return out


def coletar_de_diretorio(diretorio: Path | str | None = None) -> pd.DataFrame:
    """Lê todos os arquivos em ``dados_nordeste/raw/sefaz_ce/`` e gera consolidado."""
    diretorio = Path(diretorio) if diretorio else (RAW_DIR / "sefaz_ce")
    if not diretorio.exists():
        logger.warning(
            f"Diretório {diretorio} não existe. Baixe arquivos do Ceará Transparente "
            "manualmente (ICMS/IPVA cota-parte por município) e coloque-os lá."
        )
        return pd.DataFrame()

    arquivos = [
        p for p in diretorio.rglob("*")
        if p.is_file() and p.suffix.lower() in {".csv", ".xlsx", ".xls"}
    ]
    if not arquivos:
        logger.warning(f"SEFAZ-CE: nenhum arquivo em {diretorio}.")
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
        bruto = bruto.merge(info[["cod_ibge", "regiao_codigo", "regiao_nome"]], on="cod_ibge", how="left")
    bruto = bruto.dropna(subset=["cod_ibge"])

    pivot = bruto.pivot_table(
        index=["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes"],
        columns="tipo_canon",
        values="valor",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    pivot.columns.name = None

    for col in ["icms_dest", "ipva_dest", "fpe_dest", "fundeb_estado_dest",
                "royalties_estado_dest", "outros_dest"]:
        if col not in pivot.columns:
            pivot[col] = 0

    cols_total = [c for c in pivot.columns if c.endswith("_dest")]
    pivot["total"] = pivot[cols_total].sum(axis=1)
    pivot = pivot.sort_values(["ano", "mes", "cod_ibge"]).reset_index(drop=True)

    save_dataframe(
        pivot,
        "transf_estaduais_ce_mensal",
        subdir="processed",
        path_parts=["sefaz_ce"],
    )
    logger.info(
        f"SEFAZ-CE consolidado: {len(pivot)} linhas, {pivot['cod_ibge'].nunique()} municípios"
    )
    return pivot


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_de_diretorio()

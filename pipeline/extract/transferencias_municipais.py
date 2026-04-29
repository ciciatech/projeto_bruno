"""
Coletor de transferências constitucionais federais para municípios cearenses.

Fonte: dataset CKAN do Tesouro Nacional/STN
``transferencias-constitucionais-para-municipios``. Inclui FPM, FUNDEB
cota-União, ITR, IPI-Exportação, ICMS Desoneração (LC 87/96), IOF-Ouro, FEX.

O CKAN publica:
- 1 CSV anual por ano até 2015 (e parte de 2016)
- 1 CSV mensal a partir de 2016-08

Schema dos CSVs (separador ``;``, encoding latin-1):
    Município;UF;ANO;Mês;1º Decêndio;2º Decêndio;3º Decêndio;Item transferência;Transferência

Não há código IBGE — fazemos merge por nome de município com ``pipeline/regioes_ce.py``.

Cada município/mês aparece em várias linhas: cada origem (Item: FPM, ITR,
IPI-EXP, IPVA, ICMS, FPE, etc.) gera linhas para um ou mais destinos
(Transferência: FPM, FUNDEB, ITR, Royalties). Pivotamos pelo **destino** porque
é o valor que efetivamente entra no caixa municipal.

Saída: ``dados_nordeste/processed/transferencias_municipais/transf_constitucionais_ce_mensal.csv``
com colunas (cod_ibge, regiao_codigo, regiao_nome, ano, mes,
fpm_dest, fundeb_dest, itr_dest, royalties_dest, total).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import (
    PERIODO_INICIO_MENSAL,
    PERIODO_FIM_MENSAL,
    RAW_DIR,
    REQUEST_TIMEOUT,
)
from pipeline.extract._portal_transp_csv import _info_municipios_ce_indexado
from pipeline.regioes_ce import _normalizar_nome
from pipeline.utils import save_dataframe, safe_request

logger = logging.getLogger(__name__)

CKAN_PACKAGE_URL = (
    "https://www.tesourotransparente.gov.br/ckan/api/3/action/"
    "package_show?id=transferencias-constitucionais-para-municipios"
)


def _listar_resources_por_periodo(
    ano_inicio: int, ano_fim: int
) -> dict[tuple[int, int | None], dict]:
    """
    Consulta o CKAN e retorna {(ano, mes_ou_None): resource} cobrindo o período.
    Quando há arquivo mensal e anual para o mesmo período, prefere o mensal.
    Quando há duplicatas para o mesmo (ano, mes), prefere o resource mais novo.
    """
    resp = safe_request(CKAN_PACKAGE_URL, timeout=REQUEST_TIMEOUT)
    if resp is None:
        raise RuntimeError("Falha ao consultar CKAN STN")
    data = resp.json()["result"]
    csv_resources = [r for r in data["resources"] if r["format"] == "CSV"]

    out: dict[tuple[int, int | None], dict] = {}
    for r in csv_resources:
        url = r["url"]
        m_mes = re.search(r"(\d{4})(\d{2})\.csv", url, re.IGNORECASE)
        m_ano = re.search(r"(\d{4})\.csv", url, re.IGNORECASE)
        if m_mes:
            ano = int(m_mes.group(1))
            mes = int(m_mes.group(2))
            chave = (ano, mes)
        elif m_ano:
            ano = int(m_ano.group(1))
            chave = (ano, None)
        else:
            continue
        if not (ano_inicio <= ano <= ano_fim):
            continue
        existing = out.get(chave)
        lm_new = r.get("last_modified") or r.get("created") or ""
        lm_old = (
            (existing.get("last_modified") or existing.get("created") or "")
            if existing
            else ""
        )
        if existing is None or lm_new > lm_old:
            out[chave] = r
    return out


def _baixar_resource(resource: dict, destino: Path) -> Path | None:
    """Baixa um resource CKAN para destino, com cache."""
    if destino.exists() and destino.stat().st_size > 100_000:
        return destino
    destino.parent.mkdir(parents=True, exist_ok=True)
    url = resource["url"]
    logger.info(f"  download: {url}")
    try:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT * 5) as r:
            r.raise_for_status()
            with open(destino, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
    except Exception as e:
        logger.error(f"  download falhou: {e}")
        if destino.exists():
            destino.unlink()
        return None
    return destino


# Mapeamento "Transferência" (destino — o que o município recebe) → coluna canônica
_MAPA_DESTINO = {
    "FPM": "fpm_dest",
    "FUNDEB": "fundeb_dest",
    "ITR": "itr_dest",
    "ROYALTIES": "royalties_dest",
}


def _mapear_destino(dest: str) -> str:
    if not isinstance(dest, str):
        return "outros_dest"
    s = dest.strip().upper()
    return _MAPA_DESTINO.get(s, "outros_dest")


def _processar_csv_ce(path: Path) -> pd.DataFrame:
    """Lê um CSV do STN, filtra UF=CE, agrega por (município, ano, mês, destino)."""
    df = pd.read_csv(path, sep=";", encoding="latin-1", dtype=str)

    # Renomear colunas para nomes canônicos (sem acento, lowercase)
    rename = {}
    for c in df.columns:
        s = _normalizar_nome(c)
        if s.startswith("munic"):
            rename[c] = "municipio"
        elif s == "uf":
            rename[c] = "uf"
        elif s == "ano":
            rename[c] = "ano"
        elif s in {"mes"}:
            rename[c] = "mes"
        elif "decendio" in s and s.startswith("1"):
            rename[c] = "dec1"
        elif "decendio" in s and s.startswith("2"):
            rename[c] = "dec2"
        elif "decendio" in s and s.startswith("3"):
            rename[c] = "dec3"
        elif "transferencia" in s and "item" not in s:
            rename[c] = "destino"
        elif "item" in s:
            rename[c] = "item"
    df = df.rename(columns=rename)

    obrig = ["municipio", "uf", "ano", "mes", "destino"]
    falt = [c for c in obrig if c not in df.columns]
    if falt:
        raise ValueError(f"Colunas ausentes em {path.name}: {falt}. Colunas: {list(df.columns)}")

    ce = df[df["uf"].astype(str).str.strip().str.upper() == "CE"].copy()
    if ce.empty:
        return pd.DataFrame()

    ce["_chave"] = ce["municipio"].apply(_normalizar_nome)
    ce["ano"] = pd.to_numeric(ce["ano"], errors="coerce").astype("Int64")
    ce["mes"] = pd.to_numeric(ce["mes"], errors="coerce").astype("Int64")
    for col in ["dec1", "dec2", "dec3"]:
        if col in ce.columns:
            ce[col] = pd.to_numeric(
                ce[col].astype(str).str.replace(",", "."), errors="coerce"
            ).fillna(0)
        else:
            ce[col] = 0
    ce["valor"] = ce["dec1"] + ce["dec2"] + ce["dec3"]
    ce["dest_canon"] = ce["destino"].apply(_mapear_destino)

    return ce.groupby(
        ["_chave", "ano", "mes", "dest_canon"], as_index=False
    )["valor"].sum()


def coletar_ce(
    ano_inicio: int = PERIODO_INICIO_MENSAL,
    ano_fim: int = PERIODO_FIM_MENSAL,
    manter_csv: bool = False,
) -> pd.DataFrame:
    """Baixa, processa e consolida transferências constitucionais para municípios CE."""
    raw_dir = RAW_DIR / "transferencias_municipais"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = raw_dir / "_consolidado_por_arquivo"
    cache_dir.mkdir(exist_ok=True)

    resources = _listar_resources_por_periodo(ano_inicio, ano_fim)
    logger.info(f"STN: {len(resources)} arquivos CSV para 2015-{ano_fim}")

    frames = []
    # Ordenar tratando mes=None (arquivos anuais) como 0 para vir antes dos mensais
    def _ord(item):
        a, m = item[0]
        return (a, m if m is not None else 0)
    for chave, r in sorted(resources.items(), key=_ord):
        ano, mes = chave
        nome_chave = f"{ano:04d}{mes:02d}" if mes else f"{ano:04d}"
        cache_csv = cache_dir / f"{nome_chave}.csv"
        if cache_csv.exists():
            frames.append(pd.read_csv(cache_csv))
            continue

        path_dl = raw_dir / f"{nome_chave}_raw.csv"
        baixado = _baixar_resource(r, path_dl)
        if baixado is None:
            continue

        try:
            agg = _processar_csv_ce(baixado)
            if agg.empty:
                logger.warning(f"  [{nome_chave}] sem dados CE")
            else:
                agg.to_csv(cache_csv, index=False)
                frames.append(agg)
                logger.info(
                    f"  [{nome_chave}] {len(agg)} linhas, "
                    f"R$ {agg['valor'].sum():,.2f}"
                )
        except Exception as e:
            logger.error(f"  [{nome_chave}] erro processando: {e}")
        finally:
            if not manter_csv:
                baixado.unlink(missing_ok=True)

    if not frames:
        logger.warning("Transferências municipais STN: nada coletado.")
        return pd.DataFrame()

    bruto = pd.concat(frames, ignore_index=True)
    pivot = bruto.pivot_table(
        index=["_chave", "ano", "mes"],
        columns="dest_canon",
        values="valor",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    pivot.columns.name = None

    for col in ["fpm_dest", "fundeb_dest", "itr_dest", "royalties_dest", "outros_dest"]:
        if col not in pivot.columns:
            pivot[col] = 0

    cols_total = [c for c in pivot.columns if c not in {"_chave", "ano", "mes"}]
    pivot["total"] = pivot[cols_total].sum(axis=1)

    info = _info_municipios_ce_indexado()
    out = pivot.merge(info, on="_chave", how="left").dropna(subset=["cod_ibge"])
    out = out.drop(columns=["_chave"])

    cols_first = ["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes"]
    cols_rest = [c for c in out.columns if c not in cols_first]
    out = out[cols_first + cols_rest].sort_values(
        ["ano", "mes", "cod_ibge"]
    ).reset_index(drop=True)

    save_dataframe(
        out,
        "transf_constitucionais_ce_mensal",
        subdir="processed",
        path_parts=["transferencias_municipais"],
    )
    logger.info(
        f"STN consolidado: {len(out)} linhas, {out['cod_ibge'].nunique()} municípios CE, "
        f"{out.groupby(['ano','mes']).ngroups} meses, total R$ {out['total'].sum():,.2f}"
    )
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

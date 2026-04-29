"""
Helpers compartilhados para coletores que consomem CSVs bulk do Portal da Transparência.

Padrão dos arquivos:
- URL: ``portaldatransparencia.gov.br/download-de-dados/{programa}/{AAAAMM}``
- Resposta: ZIP único contendo um CSV grande (300 MB - 2.5 GB)
- Encoding: latin-1; separador ``;``; decimal ``,``
- Colunas-chave: ``UF``, ``CODIGO MUNICIPIO SIAFI``, ``NOME MUNICIPIO``, ``VALOR PARCELA``
  (nomes podem variar levemente entre programas; detectamos por slug)

Por causa do tamanho, processamos em chunks de 100k linhas com filtro ``UF=CE``
aplicado antes de qualquer concatenação. O ZIP cru fica em
``dados_nordeste/raw/{programa}/`` e o consolidado mensal em
``dados_nordeste/processed/{programa}/``.
"""

from __future__ import annotations

import io
import logging
import re
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import RAW_DIR, REQUEST_TIMEOUT
from pipeline.regioes_ce import _normalizar_nome, get_regiao_info

logger = logging.getLogger(__name__)

CHUNK_LINES = 100_000


def _slug_col(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _detectar_colunas_pt(headers: list[str]) -> dict[str, str]:
    """Mapeia colunas do CSV Portal da Transparência para nomes canônicos."""
    canon: dict[str, str] = {}
    for col in headers:
        s = _slug_col(col)
        if s.startswith("mes_competencia") or s == "mes_compet":
            canon["mes_competencia"] = col
        elif s.startswith("mes_referencia"):
            canon["mes_referencia"] = col
        elif s == "uf":
            canon["uf"] = col
        elif "municipio_siafi" in s or "codigo_siafi" in s:
            canon["cod_siafi_mun"] = col
        elif s in {"nome_municipio", "municipio"}:
            canon["nome_municipio"] = col
        elif s.startswith("valor_parcela") or s == "valor":
            canon["valor"] = col
        elif s.startswith("valor_beneficio"):
            canon["valor"] = col
        elif s.startswith("valor_transferido"):
            canon["valor"] = col
        elif s == "nis_favorecido":
            canon["nis"] = col
    return canon


def baixar_zip_mensal(
    url: str, destino_zip: Path, force: bool = False
) -> Path | None:
    """
    Baixa o ZIP mensal do Portal da Transparência se ainda não existir.
    Retorna o Path do ZIP ou None em caso de falha.
    """
    if destino_zip.exists() and destino_zip.stat().st_size > 1_000_000 and not force:
        logger.debug(f"  cache hit: {destino_zip.name}")
        return destino_zip

    destino_zip.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"  download: {url}")
    try:
        with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT * 5) as resp:
            resp.raise_for_status()
            with open(destino_zip, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
    except Exception as e:
        logger.error(f"  download falhou ({url}): {e}")
        if destino_zip.exists():
            destino_zip.unlink()
        return None

    if destino_zip.stat().st_size < 1_000_000:
        logger.warning(
            f"  arquivo {destino_zip} muito pequeno "
            f"({destino_zip.stat().st_size} bytes) — provavelmente erro"
        )
        destino_zip.unlink()
        return None

    return destino_zip


def _info_municipios_ce_indexado() -> pd.DataFrame:
    """Lookup table {nome_normalizado → (cod_ibge, regiao_codigo, regiao_nome)}."""
    df = get_regiao_info()[["cod_ibge", "nome_ibge", "regiao_codigo", "regiao_nome"]].copy()
    df["_chave"] = df["nome_ibge"].apply(_normalizar_nome)
    return df[["_chave", "cod_ibge", "regiao_codigo", "regiao_nome"]]


def consolidar_mes_ce(zip_path: Path, ano: int, mes: int) -> pd.DataFrame:
    """
    Lê o CSV interno do ZIP em chunks, filtra UF=CE, agrega por município e
    mescla com o cadastro de regiões para obter cod_ibge oficial de 7 dígitos.
    """
    with zipfile.ZipFile(zip_path) as zf:
        nomes_csv = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not nomes_csv:
            logger.error(f"  ZIP sem CSV: {zip_path}")
            return pd.DataFrame()

        # Detectar colunas via primeiro chunk de 1 linha
        with zf.open(nomes_csv[0]) as raw:
            preview = pd.read_csv(raw, sep=";", encoding="latin-1", dtype=str, nrows=1)
        canon = _detectar_colunas_pt(list(preview.columns))

        obrig = ["uf", "valor", "nome_municipio"]
        falt = [c for c in obrig if c not in canon]
        if falt:
            logger.error(
                f"  Colunas ausentes: {falt}. Headers: {list(preview.columns)}"
            )
            return pd.DataFrame()

        chunk_aggs: list[pd.DataFrame] = []
        with zf.open(nomes_csv[0]) as raw:
            reader = pd.read_csv(
                raw,
                sep=";",
                encoding="latin-1",
                dtype=str,
                chunksize=CHUNK_LINES,
            )
            for chunk in reader:
                ce = chunk[chunk[canon["uf"]].str.strip().str.upper() == "CE"]
                if ce.empty:
                    continue
                ce = ce.copy()
                ce["_chave"] = ce[canon["nome_municipio"]].apply(_normalizar_nome)
                ce["_valor"] = pd.to_numeric(
                    ce[canon["valor"]]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False),
                    errors="coerce",
                ).fillna(0)
                agg = ce.groupby("_chave", as_index=False).agg(
                    valor_total=("_valor", "sum"),
                    beneficiarios=("_valor", "count"),
                )
                chunk_aggs.append(agg)

    if not chunk_aggs:
        return pd.DataFrame()

    consolidado = (
        pd.concat(chunk_aggs, ignore_index=True)
        .groupby("_chave", as_index=False)
        .agg(valor_total=("valor_total", "sum"), beneficiarios=("beneficiarios", "sum"))
        .merge(_info_municipios_ce_indexado(), on="_chave", how="left")
    )

    nao_casou = consolidado[consolidado["cod_ibge"].isna()]
    if not nao_casou.empty:
        logger.warning(
            f"  {len(nao_casou)} nomes CE não casaram: "
            f"{nao_casou['_chave'].head(5).tolist()}"
        )

    consolidado = consolidado.dropna(subset=["cod_ibge"]).copy()
    consolidado["ano"] = ano
    consolidado["mes"] = mes
    return consolidado[
        ["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes",
         "valor_total", "beneficiarios"]
    ].sort_values("cod_ibge").reset_index(drop=True)


def coletar_programa_mensal(
    nome_programa: str,
    url_path: str,
    ano_inicio: int,
    ano_fim: int,
    mes_inicio: int = 1,
    mes_fim_ano_corrente: int = 12,
    manter_zip: bool = False,
    pular_meses_existentes: bool = True,
) -> pd.DataFrame:
    """
    Coleta um programa de transferência (BF/Auxílio Brasil/Novo BF/BPC) mensal
    do Portal da Transparência para todos os meses no período, filtrado para CE.

    Args:
        nome_programa: ID do programa (usado em path/output). Ex: "novo_bolsa_familia".
        url_path: segmento da URL (ex: "novo-bolsa-familia").
        ano_inicio, ano_fim: período inclusivo.
        manter_zip: se True, mantém ZIPs em dados_nordeste/raw/{nome_programa}/.
        pular_meses_existentes: se True, não rebaixa meses já consolidados.

    Returns:
        DataFrame consolidado mensal (ano × mês × cod_ibge × valor × beneficiarios).
    """
    base_url = f"https://portaldatransparencia.gov.br/download-de-dados/{url_path}"
    raw_dir = RAW_DIR / nome_programa
    raw_dir.mkdir(parents=True, exist_ok=True)

    cache_consolidado = raw_dir / "_meses_consolidados"
    cache_consolidado.mkdir(exist_ok=True)

    frames = []
    for ano in range(ano_inicio, ano_fim + 1):
        for mes in range(1, 13):
            if ano == ano_inicio and mes < mes_inicio:
                continue
            if ano == ano_fim and mes > mes_fim_ano_corrente:
                continue
            chave = f"{ano:04d}{mes:02d}"
            cache_csv = cache_consolidado / f"{chave}.csv"
            if pular_meses_existentes and cache_csv.exists():
                logger.info(f"  [{nome_programa} {chave}] cache consolidado")
                frames.append(pd.read_csv(cache_csv, dtype={"cod_ibge": str, "regiao_codigo": str}))
                continue

            url = f"{base_url}/{chave}"
            zip_path = raw_dir / f"{chave}.zip"
            zip_baixado = baixar_zip_mensal(url, zip_path)
            if zip_baixado is None:
                logger.warning(f"  [{nome_programa} {chave}] download falhou — pulando")
                continue

            df_mes = consolidar_mes_ce(zip_baixado, ano, mes)
            if df_mes.empty:
                logger.warning(f"  [{nome_programa} {chave}] nenhum dado CE")
                continue

            df_mes.to_csv(cache_csv, index=False)
            frames.append(df_mes)
            logger.info(
                f"  [{nome_programa} {chave}] {len(df_mes)} municípios, "
                f"R$ {df_mes['valor_total'].sum():,.2f}"
            )

            if not manter_zip:
                zip_baixado.unlink()

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True).sort_values(
        ["ano", "mes", "cod_ibge"]
    ).reset_index(drop=True)

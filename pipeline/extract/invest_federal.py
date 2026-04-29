"""
Coletor de investimento federal direto no Ceará — metodologia Prof. Paulo (abr/2026).

Conforme áudio do Prof. Paulo, o investimento federal no CE é a soma de **três
componentes** extraídos da execução orçamentária federal (RREO da União, via CSV
bulk do Portal da Transparência ``despesas-execucao``):

1. **(a) Direto-CE** — aplicações diretas com UF de execução = "CE".
2. **(b) NE-rateado** — aplicações diretas executadas no Nordeste sem UF
   identificada (subtítulo/localizador regional NE), rateadas pelo share do PIB
   CE no NE (~14,5%, conforme ``pib_shares.share_ce_nordeste``).
3. **(c) Nacional-rateado** — aplicações diretas sem UF e sem região
   identificada (localizador "NACIONAL"), rateadas pelo share do PIB CE no
   Brasil (~2,2%, conforme ``pib_shares.share_ce_brasil``).

Filtro fixo: ``Código Grupo de Despesa = 4`` (Investimentos) e
``Código Modalidade da Despesa = 90`` (Aplicações Diretas — exclui transferências
para evitar dupla contagem com SIOF estadual/municipal).

Saída: ``dados_nordeste/processed/invest_federal/invest_federal_ce_mensal.csv``
com colunas (ano, mes, direto_ce, ne_rateado, nacional_rateado, total).

Atenção: esse coletor produz valores **estaduais agregados**, não regionais.
Para o painel regional, o valor é replicado nas 14 regiões; quando o pesquisador
quiser regionalizar, multiplica pelo share regional dentro do PIB CE.
"""

from __future__ import annotations

import io
import logging
import re
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd

from pipeline.config import (
    PERIODO_INICIO_MENSAL,
    PERIODO_FIM_MENSAL,
    RAW_DIR,
)
from pipeline.extract._portal_transp_csv import baixar_zip_mensal
from pipeline.pib_shares import share_ce_brasil, share_ce_nordeste
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)


CHUNK_LINES = 100_000
COD_GRUPO_INVESTIMENTOS = "4"
COD_MODALIDADE_APLICACAO_DIRETA = "90"


def _slug(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _detectar(headers: list[str]) -> dict[str, str]:
    """Mapeia colunas do CSV de despesas-execucao para nomes canônicos."""
    canon: dict[str, str] = {}
    for col in headers:
        s = _slug(col)
        if s == "uf":
            canon["uf"] = col
        elif s == "municipio":
            canon["municipio"] = col
        elif s == "codigo_grupo_de_despesa":
            canon["cod_grupo"] = col
        elif s == "codigo_modalidade_da_despesa":
            canon["cod_modalidade"] = col
        elif s == "valor_empenhado_r" or s == "valor_empenhado":
            canon["valor_empenhado"] = col
        elif s == "valor_pago_r" or s == "valor_pago":
            canon["valor_pago"] = col
        elif s == "valor_liquidado_r" or s == "valor_liquidado":
            canon["valor_liquidado"] = col
        elif s == "sigla_localizador":
            canon["sigla_localizador"] = col
        elif s == "nome_localizador":
            canon["nome_localizador"] = col
        elif s == "nome_subtitulo":
            canon["nome_subtitulo"] = col
        elif "ano_e_mes" in s or s == "ano_mes":
            canon["ano_mes"] = col
    return canon


def _safe_str(v) -> str:
    """Converte qualquer valor para string segura (NaN/None → '')."""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v)


def _classificar_componente(uf, sigla_loc, nome_loc, nome_sub) -> str:
    """
    Classifica cada linha em uma das três categorias do Prof. Paulo:
      - 'direto_ce'        : UF = CE
      - 'ne_rateado'       : sem UF mas Localizador/Subtítulo indica Nordeste
      - 'nacional_rateado' : sem UF e sem região (Localizador NACIONAL)
      - 'outro'            : outro estado/região (descartado)
    """
    uf_clean = _safe_str(uf).strip().upper()
    if uf_clean == "CE":
        return "direto_ce"
    if uf_clean and uf_clean not in {"-", "ND", "NA"}:
        return "outro"

    texto = " ".join(_safe_str(x) for x in [sigla_loc, nome_loc, nome_sub]).upper()
    if "NORDESTE" in texto or " NE " in f" {texto} " or texto.endswith(" NE"):
        return "ne_rateado"
    if "NACIONAL" in texto or "BRASIL" in texto or "TODOS" in texto or texto.strip() == "":
        return "nacional_rateado"
    return "outro"


def _consolidar_mes(zip_path: Path, ano: int, mes: int) -> pd.DataFrame:
    """Lê CSV de despesas-execucao e classifica linhas em (a), (b), (c) ou descarta."""
    with zipfile.ZipFile(zip_path) as zf:
        nomes_csv = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not nomes_csv:
            return pd.DataFrame()
        with zf.open(nomes_csv[0]) as raw:
            preview = pd.read_csv(raw, sep=";", encoding="latin-1", dtype=str, nrows=1)
        canon = _detectar(list(preview.columns))

        obrig = ["uf", "cod_grupo", "cod_modalidade", "valor_pago"]
        falt = [c for c in obrig if c not in canon]
        if falt:
            logger.error(f"  [{ano}/{mes:02d}] colunas ausentes: {falt}")
            return pd.DataFrame()

        chunk_aggs: list[pd.DataFrame] = []
        with zf.open(nomes_csv[0]) as raw:
            reader = pd.read_csv(
                raw, sep=";", encoding="latin-1", dtype=str, chunksize=CHUNK_LINES
            )
            for chunk in reader:
                inv = chunk[
                    (chunk[canon["cod_grupo"]] == COD_GRUPO_INVESTIMENTOS)
                    & (chunk[canon["cod_modalidade"]] == COD_MODALIDADE_APLICACAO_DIRETA)
                ]
                if inv.empty:
                    continue
                inv = inv.copy()
                inv["componente"] = inv.apply(
                    lambda r: _classificar_componente(
                        r.get(canon["uf"], ""),
                        r.get(canon.get("sigla_localizador", ""), ""),
                        r.get(canon.get("nome_localizador", ""), ""),
                        r.get(canon.get("nome_subtitulo", ""), ""),
                    ),
                    axis=1,
                )
                inv = inv[inv["componente"] != "outro"]
                if inv.empty:
                    continue
                for col in ["valor_empenhado", "valor_pago", "valor_liquidado"]:
                    src = canon.get(col)
                    if src is None:
                        inv[col] = 0
                    else:
                        inv[col] = pd.to_numeric(
                            inv[src]
                            .astype(str)
                            .str.replace(".", "", regex=False)
                            .str.replace(",", ".", regex=False),
                            errors="coerce",
                        ).fillna(0)
                agg = inv.groupby("componente", as_index=False).agg(
                    valor_empenhado=("valor_empenhado", "sum"),
                    valor_pago=("valor_pago", "sum"),
                    valor_liquidado=("valor_liquidado", "sum"),
                )
                chunk_aggs.append(agg)

    if not chunk_aggs:
        return pd.DataFrame()

    consolidado = (
        pd.concat(chunk_aggs, ignore_index=True)
        .groupby("componente", as_index=False)
        .agg(
            valor_empenhado=("valor_empenhado", "sum"),
            valor_pago=("valor_pago", "sum"),
            valor_liquidado=("valor_liquidado", "sum"),
        )
    )

    # Garantir presença das três categorias mesmo se zero
    for cat in ["direto_ce", "ne_rateado", "nacional_rateado"]:
        if cat not in consolidado["componente"].values:
            consolidado = pd.concat(
                [
                    consolidado,
                    pd.DataFrame(
                        [{"componente": cat, "valor_empenhado": 0,
                          "valor_pago": 0, "valor_liquidado": 0}]
                    ),
                ],
                ignore_index=True,
            )

    # Aplicar shares de rateio para (b) e (c)
    share_ne = share_ce_nordeste(ano)
    share_br = share_ce_brasil(ano)
    consolidado["valor_pago_rateado"] = consolidado.apply(
        lambda r: r["valor_pago"] * (
            1.0 if r["componente"] == "direto_ce"
            else share_ne if r["componente"] == "ne_rateado"
            else share_br
        ),
        axis=1,
    )

    out = consolidado.set_index("componente")[["valor_pago", "valor_pago_rateado"]].T
    # Pivotar: 1 linha por (ano, mes) com colunas direto_ce, ne_rateado, nacional_rateado
    pivot = pd.DataFrame({
        "ano": [ano],
        "mes": [mes],
        "direto_ce": [consolidado.loc[consolidado["componente"] == "direto_ce", "valor_pago"].sum()],
        "ne_rateado": [consolidado.loc[consolidado["componente"] == "ne_rateado", "valor_pago"].sum() * share_ne],
        "nacional_rateado": [consolidado.loc[consolidado["componente"] == "nacional_rateado", "valor_pago"].sum() * share_br],
        "ne_bruto": [consolidado.loc[consolidado["componente"] == "ne_rateado", "valor_pago"].sum()],
        "nacional_bruto": [consolidado.loc[consolidado["componente"] == "nacional_rateado", "valor_pago"].sum()],
        "share_ne": [share_ne],
        "share_br": [share_br],
    })
    pivot["total"] = pivot["direto_ce"] + pivot["ne_rateado"] + pivot["nacional_rateado"]
    return pivot


def coletar_ce(
    ano_inicio: int = PERIODO_INICIO_MENSAL,
    ano_fim: int = PERIODO_FIM_MENSAL,
    manter_zip: bool = False,
    pular_meses_existentes: bool = True,
) -> pd.DataFrame:
    """
    Coleta investimento federal mensal (3 componentes) para o CE.

    Em cada mês baixa o CSV ``despesas-execucao`` (~5 MB ZIP, ~40 MB CSV),
    filtra investimentos + aplicações diretas, classifica em CE/NE/Nacional,
    aplica shares de rateio. Salva um cache por mês para não rebaixar.
    """
    raw_dir = RAW_DIR / "invest_federal"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = raw_dir / "_meses_consolidados"
    cache_dir.mkdir(exist_ok=True)

    base_url = "https://portaldatransparencia.gov.br/download-de-dados/despesas-execucao"
    frames = []

    for ano in range(ano_inicio, ano_fim + 1):
        for mes in range(1, 13):
            chave = f"{ano:04d}{mes:02d}"
            cache_csv = cache_dir / f"{chave}.csv"
            if pular_meses_existentes and cache_csv.exists():
                logger.info(f"  [{chave}] cache hit")
                cached = pd.read_csv(cache_csv)
                if not cached.empty:
                    frames.append(cached)
                continue

            url = f"{base_url}/{chave}"
            zip_path = raw_dir / f"{chave}.zip"
            zip_baixado = baixar_zip_mensal(url, zip_path)
            if zip_baixado is None:
                continue

            try:
                df_mes = _consolidar_mes(zip_baixado, ano, mes)
                if df_mes.empty:
                    pd.DataFrame(columns=[
                        "ano", "mes", "direto_ce", "ne_rateado",
                        "nacional_rateado", "total",
                    ]).to_csv(cache_csv, index=False)
                else:
                    df_mes.to_csv(cache_csv, index=False)
                    frames.append(df_mes)
                    logger.info(
                        f"  [{chave}] direto_CE: R$ {df_mes['direto_ce'].iloc[0]:,.0f} | "
                        f"NE×{df_mes['share_ne'].iloc[0]:.3f}: R$ {df_mes['ne_rateado'].iloc[0]:,.0f} | "
                        f"NAC×{df_mes['share_br'].iloc[0]:.3f}: R$ {df_mes['nacional_rateado'].iloc[0]:,.0f} | "
                        f"total: R$ {df_mes['total'].iloc[0]:,.0f}"
                    )
            except Exception as e:
                logger.error(f"  [{chave}] erro: {e}")
            finally:
                if not manter_zip:
                    zip_baixado.unlink(missing_ok=True)

    if not frames:
        logger.warning("Invest. federal RREO: nada coletado.")
        return pd.DataFrame()

    consolidado = pd.concat(frames, ignore_index=True).sort_values(
        ["ano", "mes"]
    ).reset_index(drop=True)

    save_dataframe(
        consolidado,
        "invest_federal_ce_mensal",
        subdir="processed",
        path_parts=["invest_federal"],
    )
    logger.info(
        f"Invest. federal CE: {len(consolidado)} meses, "
        f"total acumulado: R$ {consolidado['total'].sum():,.0f}"
    )
    return consolidado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

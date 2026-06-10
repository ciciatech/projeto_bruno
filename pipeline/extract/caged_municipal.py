"""
Coletor CAGED em granularidade municipal para o Ceará.

Reaproveita os métodos de download/parse já implementados em ``caged_rais.py``
(``CagedRais._processar_caged_antigo_mes`` para 2015-2019 e
``CagedRais._processar_caged_mes`` para o Novo CAGED a partir de 2020), mas
agrega por **município × mês** em vez de UF × mês.

Essa granularidade é necessária para o painel regional do Prof. Paulo (14
regiões CE), já que a agregação município → região depende dos códigos IBGE
de 7 dígitos preservados a nível municipal.

## Custo de execução

Os arquivos do PDET/MTE no FTP são pesados (~3-5 GB/mês descomprimido).
Coleta integral 2015-2025 → ~500 GB raw + horas de download. Por isso o
módulo:

- Mantém cache de consolidados por mês em
  ``dados_nordeste/processed/caged_municipal/_meses_consolidados/``.
- Pula meses já processados (controlado por ``pular_meses_existentes``).
- Aplica filtro CE (código IBGE começa com 23) imediatamente após o parse,
  descartando os demais NE para reduzir memória.

Saída: ``dados_nordeste/processed/caged_municipal/caged_municipal_ce_mensal.csv``
com (cod_ibge, regiao_codigo, regiao_nome, ano, mes, admissoes, desligamentos,
saldo, total_movimentacoes, salario_medio).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import (
    PERIODO_INICIO_MENSAL,
    PERIODO_FIM_MENSAL,
    PROCESSED_DIR,
)
from pipeline.extract.caged_rais import CagedRais
from pipeline.regioes_ce import get_regiao_info
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

# Início do Novo CAGED (eSocial). Antes disso, layout antigo.
NOVO_CAGED_INICIO = 2020


def _agregar_municipal_ce(df: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    """
    Aplica filtro CE e agrega por (cod_ibge, ano, mes).

    O DataFrame de entrada vem do ``_processar_caged_*_mes`` e tem ao menos:
    municipio (código IBGE 7 dígitos), saldo_movimentacao, salario.
    """
    if df is None or df.empty or "municipio" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["municipio"] = df["municipio"].astype(str).str.replace(r"\D", "", regex=True)
    # Filtrar CE (código IBGE inicia com 23)
    df = df[df["municipio"].str.startswith("23")]
    if df.empty:
        return pd.DataFrame()

    # Códigos podem vir com 6 dígitos (padrão CAGED/PDET, sem o dígito
    # verificador) ou 7 (código IBGE completo). O 7º dígito é VERIFICADOR e
    # não pode ser inventado (ex.: Abaiara 230010 → 2300101, não 2300100).
    # Mapeamos prefixo-6 → código oficial de 7 dígitos a partir dos 184
    # municípios CE — os prefixos de 6 dígitos são únicos entre eles.
    cods_validos = set(get_regiao_info()["cod_ibge"])
    map_6to7 = {c[:6]: c for c in cods_validos}
    df["cod_ibge"] = df["municipio"].apply(
        lambda x: x if len(x) == 7 else map_6to7.get(x, x)
    )

    mask_validos = df["cod_ibge"].isin(cods_validos)
    if not mask_validos.all():
        descartados = df.loc[~mask_validos, "cod_ibge"]
        logger.warning(
            f"CAGED-mun {ano}/{mes:02d}: descartando {len(descartados)} linhas "
            f"com código IBGE fora dos 184 municípios CE "
            f"({descartados.nunique()} códigos: {sorted(descartados.unique())[:20]})"
        )
    df = df[mask_validos]
    if df.empty:
        return pd.DataFrame()

    if "saldo_movimentacao" not in df.columns:
        logger.warning(f"CAGED {ano}/{mes:02d}: sem coluna saldo_movimentacao")
        return pd.DataFrame()

    df["saldo_movimentacao"] = pd.to_numeric(df["saldo_movimentacao"], errors="coerce")
    if "salario" not in df.columns:
        df["salario"] = pd.NA
    else:
        df["salario"] = pd.to_numeric(df["salario"], errors="coerce")

    agg = df.groupby("cod_ibge", as_index=False).agg(
        admissoes=("saldo_movimentacao", lambda x: (x == 1).sum()),
        desligamentos=("saldo_movimentacao", lambda x: (x == -1).sum()),
        saldo=("saldo_movimentacao", "sum"),
        total_movimentacoes=("saldo_movimentacao", "count"),
        salario_medio=("salario", "mean"),
    )
    agg["ano"] = ano
    agg["mes"] = mes

    info = get_regiao_info()[["cod_ibge", "regiao_codigo", "regiao_nome"]]
    return agg.merge(info, on="cod_ibge", how="left")[
        ["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes",
         "admissoes", "desligamentos", "saldo", "total_movimentacoes", "salario_medio"]
    ]


def coletar_ce(
    ano_inicio: int = PERIODO_INICIO_MENSAL,
    ano_fim: int = PERIODO_FIM_MENSAL,
    pular_meses_existentes: bool = True,
) -> pd.DataFrame:
    """
    Coleta CAGED mensal por município CE no período. Combina layouts antigo e novo.
    """
    cache_dir = PROCESSED_DIR / "caged_municipal" / "_meses_consolidados"
    cache_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    for ano in range(ano_inicio, ano_fim + 1):
        for mes in range(1, 13):
            chave = f"{ano:04d}{mes:02d}"
            cache_csv = cache_dir / f"{chave}.csv"
            if pular_meses_existentes and cache_csv.exists():
                logger.info(f"  CAGED-mun [{chave}] cache hit")
                cached = pd.read_csv(cache_csv, dtype={"cod_ibge": str, "regiao_codigo": str})
                if not cached.empty:
                    frames.append(cached)
                continue

            try:
                if ano < NOVO_CAGED_INICIO:
                    df = CagedRais._processar_caged_antigo_mes(ano, mes)
                else:
                    df = CagedRais._processar_caged_mes(ano, mes)
            except Exception as e:
                logger.warning(f"  CAGED-mun [{chave}] erro: {e}")
                continue

            agg = _agregar_municipal_ce(df, ano, mes)
            if agg.empty:
                # Mês vazio aqui significa falha upstream (download/extração
                # engolida por _processar_caged_*), nunca ausência real de
                # movimentações no CE — NÃO cachear, para o resume re-tentar.
                logger.warning(f"  CAGED-mun [{chave}] sem dados CE — não cacheado")
                continue

            agg.to_csv(cache_csv, index=False)
            frames.append(agg)
            logger.info(
                f"  CAGED-mun [{chave}] {len(agg)} municípios, "
                f"saldo total: {agg['saldo'].sum():+,.0f}"
            )

    if not frames:
        logger.warning("CAGED municipal: nada coletado.")
        return pd.DataFrame()

    consolidado = pd.concat(frames, ignore_index=True).sort_values(
        ["ano", "mes", "cod_ibge"]
    ).reset_index(drop=True)

    save_dataframe(
        consolidado,
        "caged_municipal_ce_mensal",
        subdir="processed",
        path_parts=["caged_municipal"],
    )
    logger.info(
        f"CAGED municipal CE: {len(consolidado)} linhas, "
        f"{consolidado['cod_ibge'].nunique()} municípios, "
        f"{consolidado.groupby(['ano','mes']).ngroups} meses."
    )
    return consolidado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

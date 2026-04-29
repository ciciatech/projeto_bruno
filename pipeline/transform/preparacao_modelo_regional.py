"""
Construção do painel modelo-pronto **regional** (14 regiões CE × meses).

Diferentemente do ``preparacao_modelo.py`` (UF × bimestre — pipeline legado),
este módulo monta o painel solicitado pelo Prof. Paulo (abr/2026) com:

- **Endógenas**: emprego (CAGED) e salário médio
- **Variável de impacto**: investimento estadual em obras/equipamentos (SIOF)
- **Controles regionais**: invest. municipal, invest. federal voluntárias,
  Bolsa Família, BPC, transferências constitucionais STN, transferências
  estaduais SEFAZ-CE, crédito BNB ESTBAN
- **Controle estadual**: IBCR-CE (replicado para todas as regiões)

Cada fonte municipal é agregada para região via ``regioes_ce.agregar_para_regiao``.
Fontes que já vêm em região (SIOF) ou em UF/estado (IBCR-CE) entram diretamente.

Output: ``dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv``
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
from pipeline.pib_shares import share_ce_brasil
from pipeline.regioes_ce import REGIOES_CE, agregar_para_regiao

logger = logging.getLogger(__name__)

MODEL_READY_DIR = PROCESSED_DIR / "model_ready"


def _ler_csv_se_existir(path: Path) -> pd.DataFrame:
    if not path.exists():
        logger.warning(f"  ausente: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype={"cod_ibge": str, "regiao_codigo": str})
    logger.info(f"  lido: {path.name} ({len(df)} linhas)")
    return df


def _agregar_se_municipal(
    df: pd.DataFrame, cols_valor: list[str], prefixo: str
) -> pd.DataFrame:
    """Agrega DataFrame para região se vier em granularidade municipal."""
    if df.empty:
        return pd.DataFrame()
    if "cod_ibge" in df.columns and "regiao_codigo" not in df.columns:
        # municipal puro — aplica agregação
        ag = agregar_para_regiao(
            df, "cod_ibge", cols_valor, cols_groupby_extra=["ano", "mes"]
        )
    elif "regiao_codigo" in df.columns:
        # já tem região; só consolidar se houver duplicatas município → região
        ag = (
            df.groupby(["regiao_codigo", "regiao_nome", "ano", "mes"], as_index=False)[
                cols_valor
            ]
            .sum(min_count=1)
        )
    else:
        logger.warning(f"DataFrame sem cod_ibge/regiao_codigo — pulando")
        return pd.DataFrame()

    ag = ag.rename(columns={c: f"{prefixo}_{c}" for c in cols_valor})
    return ag


def _carregar_caged_municipal() -> pd.DataFrame:
    df = _ler_csv_se_existir(
        PROCESSED_DIR / "caged_municipal" / "caged_municipal_ce_mensal.csv"
    )
    if df.empty:
        return df
    cols = ["admissoes", "desligamentos", "saldo", "total_movimentacoes"]
    ag = agregar_para_regiao(
        df, "cod_ibge", cols, cols_groupby_extra=["ano", "mes"]
    )
    # Salário: média ponderada por movimentações
    if "salario_medio" in df.columns and "total_movimentacoes" in df.columns:
        df = df.copy()
        df["_peso"] = pd.to_numeric(df["total_movimentacoes"], errors="coerce").fillna(0)
        df["_sal_x_peso"] = (
            pd.to_numeric(df["salario_medio"], errors="coerce").fillna(0) * df["_peso"]
        )
        ag_sal = agregar_para_regiao(
            df, "cod_ibge", ["_sal_x_peso", "_peso"], cols_groupby_extra=["ano", "mes"]
        )
        ag_sal["salario_medio"] = ag_sal["_sal_x_peso"] / ag_sal["_peso"].replace(
            0, pd.NA
        )
        ag_sal = ag_sal.drop(columns=["_sal_x_peso", "_peso"])
        ag = ag.merge(
            ag_sal[["regiao_codigo", "ano", "mes", "salario_medio"]],
            on=["regiao_codigo", "ano", "mes"],
            how="left",
        )
    return ag


def _carregar_bf() -> pd.DataFrame:
    df = _ler_csv_se_existir(
        PROCESSED_DIR / "bolsa_familia" / "bf_municipio_ce_mensal.csv"
    )
    return _agregar_se_municipal(df, ["valor_total", "beneficiarios"], "bf")


def _carregar_bpc() -> pd.DataFrame:
    df = _ler_csv_se_existir(PROCESSED_DIR / "bpc" / "bpc_municipio_ce_mensal.csv")
    return _agregar_se_municipal(df, ["valor_total", "beneficiarios"], "bpc")


def _carregar_transf_stn() -> pd.DataFrame:
    df = _ler_csv_se_existir(
        PROCESSED_DIR
        / "transferencias_municipais"
        / "transf_constitucionais_ce_mensal.csv"
    )
    if df.empty:
        return df
    cols_valor = [
        c
        for c in df.columns
        if c not in {"cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes"}
    ]
    return _agregar_se_municipal(df, cols_valor, "transf_fed")


def _carregar_transf_estadual() -> pd.DataFrame:
    df = _ler_csv_se_existir(
        PROCESSED_DIR / "sefaz_ce" / "transf_estaduais_ce_mensal.csv"
    )
    if df.empty:
        return df
    cols_valor = [
        c
        for c in df.columns
        if c not in {"cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes"}
    ]
    return _agregar_se_municipal(df, cols_valor, "transf_est")


def _carregar_invest_federal() -> pd.DataFrame:
    """
    Investimento federal CE (estadual agregado) — metodologia Prof. Paulo:
    aplicações diretas RREO/Portal Transp = direto_CE + NE×0.145 + NACIONAL×0.022.
    Replicado nas 14 regiões do painel.
    """
    df = _ler_csv_se_existir(
        PROCESSED_DIR / "invest_federal" / "invest_federal_ce_mensal.csv"
    )
    if df.empty:
        return df
    out = df[["ano", "mes", "direto_ce", "ne_rateado", "nacional_rateado", "total"]].rename(
        columns={
            "direto_ce": "invest_fed_direto_ce",
            "ne_rateado": "invest_fed_ne_rateado",
            "nacional_rateado": "invest_fed_nacional_rateado",
            "total": "invest_fed_total",
        }
    )
    return out


def _carregar_invest_municipal() -> pd.DataFrame:
    df = _ler_csv_se_existir(
        PROCESSED_DIR / "invest_municipal" / "invest_municipal_ce.csv"
    )
    return _agregar_se_municipal(df, ["valor"], "invest_mun")


def _carregar_estban_bnb() -> pd.DataFrame:
    df = _ler_csv_se_existir(
        PROCESSED_DIR / "estban" / "credito_bnb_municipio_ce_mensal.csv"
    )
    if df.empty:
        return df
    cols_valor = [
        c
        for c in df.columns
        if c
        not in {"cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes"}
        and pd.api.types.is_numeric_dtype(df[c])
    ]
    if not cols_valor:
        cols_valor = ["credito_bnb_total"]
    return _agregar_se_municipal(df, cols_valor, "bnb")


def _carregar_siof_regional() -> pd.DataFrame:
    """SIOF já vem em região × ano (anual). Convertemos para mensal repetindo o valor."""
    path = PROCESSED_DIR / "execucao_orcamentaria" / "ce" / "siof_obras_regiao.csv"
    if not path.exists():
        logger.warning(f"  ausente: {path}")
        return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.lstrip("﻿") for c in df.columns]  # remover BOM
    if "regiao" not in df.columns:
        logger.warning(f"SIOF sem coluna 'regiao' — pulando")
        return pd.DataFrame()

    # Mapear nome → código
    nome_para_codigo = {v: k for k, v in REGIOES_CE.items()}
    df["regiao_codigo"] = df["regiao"].map(nome_para_codigo)
    if df["regiao_codigo"].isna().any():
        faltantes = df[df["regiao_codigo"].isna()]["regiao"].unique().tolist()
        logger.warning(f"SIOF: regiões não casaram: {faltantes}")
        df = df.dropna(subset=["regiao_codigo"])

    # Anual → mensal: replicar o valor anual em cada mês? Ou dividir por 12?
    # Tese exige uma decisão econômica. Por padrão, replicamos como flag "valor anual"
    # e o pesquisador decide depois se divide por 12 ou usa apenas para anos completos.
    out = df.rename(
        columns={"empenhado": "siof_anual_empenhado", "pago": "siof_anual_pago",
                 "dotacao": "siof_anual_dotacao", "n_acoes": "siof_anual_n_acoes"}
    )
    out = out[
        ["regiao_codigo", "ano", "siof_anual_empenhado", "siof_anual_pago",
         "siof_anual_dotacao", "siof_anual_n_acoes"]
    ]
    return out


def _carregar_invest_total_ce() -> pd.DataFrame:
    """
    Investimento total estimado para o CE = FBCF Brasil mensal × share PIB CE/Brasil.

    Hipótese explícita do Prof. Paulo (áudio abr/2026): assume-se que o share
    do investimento total CE/Brasil acompanha o share do PIB CE/Brasil
    (~2,2%, anual conforme ``pib_shares.share_ce_brasil``).

    Ver `pipeline/extract/ipea_fbcf.py` para a fonte do FBCF.
    """
    path = PROCESSED_DIR / "ipea" / "fbcf_brasil_mensal.csv"
    if not path.exists():
        logger.warning(f"  ausente: {path} — rode pipeline.extract.ipea_fbcf")
        return pd.DataFrame()
    fbcf = pd.read_csv(path)
    logger.info(f"  FBCF: lido ({len(fbcf)} obs)")
    fbcf["share"] = fbcf["ano"].apply(share_ce_brasil)
    fbcf["invest_total_ce_r_2010_mi"] = (
        fbcf["fbcf_r$_2010_milhoes"] * fbcf["share"]
    )
    return fbcf[["ano", "mes", "invest_total_ce_r_2010_mi", "share"]].rename(
        columns={"share": "share_pib_ce_br"}
    )


def _carregar_ibcr_ce() -> pd.DataFrame:
    """IBCR-CE mensal (estadual) — mesmo valor para todas as regiões."""
    from pipeline.config import RAW_DIR

    # Preferir raw/bacen_sgs_wide.csv (sempre fresh após coleta) sobre processed
    candidatos = [
        RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
    ]
    path = next((p for p in candidatos if p.exists()), None)
    if path is None:
        logger.warning("BACEN: nenhum CSV encontrado — rode pipeline.run --apenas-bacen")
        return pd.DataFrame()
    logger.info(f"  IBCR-CE: lendo de {path}")
    df = pd.read_csv(path)
    df.columns = [c.lstrip("﻿") for c in df.columns]
    if "IBCR_CE_ajuste_sazonal" not in df.columns:
        logger.warning(
            f"BACEN ({path.name}): coluna IBCR_CE_ajuste_sazonal ausente — "
            "re-rode --apenas-bacen para incluir série 25380"
        )
        return pd.DataFrame()

    df["data"] = pd.to_datetime(df["data"])
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
    return df[["ano", "mes", "IBCR_CE_ajuste_sazonal"]].rename(
        columns={"IBCR_CE_ajuste_sazonal": "ibcr_ce"}
    )


def _grade_regional_mensal(ano_inicio: int, ano_fim: int) -> pd.DataFrame:
    """Grade cartesiana 14 regiões × todos os meses do período."""
    meses = []
    for ano in range(ano_inicio, ano_fim + 1):
        for mes in range(1, 13):
            meses.append((ano, mes))
    grade = pd.MultiIndex.from_product(
        [list(REGIOES_CE.keys()), meses], names=["regiao_codigo", "_per"]
    ).to_frame(index=False)
    grade["ano"] = grade["_per"].apply(lambda x: x[0])
    grade["mes"] = grade["_per"].apply(lambda x: x[1])
    grade["regiao_nome"] = grade["regiao_codigo"].map(REGIOES_CE)
    return grade[["regiao_codigo", "regiao_nome", "ano", "mes"]]


def construir_painel(
    ano_inicio: int = PERIODO_INICIO_MENSAL,
    ano_fim: int = PERIODO_FIM_MENSAL,
) -> pd.DataFrame:
    """Monta o painel modelo-pronto regional CE."""
    MODEL_READY_DIR.mkdir(parents=True, exist_ok=True)

    grade = _grade_regional_mensal(ano_inicio, ano_fim)
    logger.info(
        f"Grade regional: {len(grade)} linhas (14 regiões × {ano_fim - ano_inicio + 1} anos)"
    )

    fontes_mensais = {
        "CAGED": _carregar_caged_municipal,
        "Bolsa Família": _carregar_bf,
        "BPC": _carregar_bpc,
        "Transf. constitucionais STN": _carregar_transf_stn,
        "Transf. estaduais SEFAZ-CE": _carregar_transf_estadual,
        "Invest. municipal (planilha)": _carregar_invest_municipal,
        "ESTBAN BNB": _carregar_estban_bnb,
    }

    painel = grade.copy()
    for nome, fn in fontes_mensais.items():
        logger.info(f"--- {nome} ---")
        df = fn()
        if df.empty:
            continue
        # garantir compatibilidade de tipos
        df["regiao_codigo"] = df["regiao_codigo"].astype(str).str.zfill(2)
        df["ano"] = df["ano"].astype(int)
        df["mes"] = df["mes"].astype(int)
        cols_drop = [c for c in ["regiao_nome"] if c in df.columns]
        if cols_drop:
            df = df.drop(columns=cols_drop)
        painel = painel.merge(
            df, on=["regiao_codigo", "ano", "mes"], how="left"
        )

    # SIOF (anual) — replicado em todos os meses do ano
    logger.info("--- SIOF (anual → mensal) ---")
    siof = _carregar_siof_regional()
    if not siof.empty:
        siof["regiao_codigo"] = siof["regiao_codigo"].astype(str).str.zfill(2)
        siof["ano"] = siof["ano"].astype(int)
        painel = painel.merge(siof, on=["regiao_codigo", "ano"], how="left")

    # Invest. federal CE (estadual agregado) — replicado nas 14 regiões
    logger.info("--- Invest. federal CE (RREO/Portal Transp) ---")
    inv_fed = _carregar_invest_federal()
    if not inv_fed.empty:
        inv_fed["ano"] = inv_fed["ano"].astype(int)
        inv_fed["mes"] = inv_fed["mes"].astype(int)
        painel = painel.merge(inv_fed, on=["ano", "mes"], how="left")

    # IBCR-CE (estadual) — replicado em todas as regiões
    logger.info("--- IBCR-CE (estadual) ---")
    ibcr = _carregar_ibcr_ce()
    if not ibcr.empty:
        ibcr["ano"] = ibcr["ano"].astype(int)
        ibcr["mes"] = ibcr["mes"].astype(int)
        painel = painel.merge(ibcr, on=["ano", "mes"], how="left")

    # Invest. total CE estimado via FBCF × share PIB (estadual, replicado)
    logger.info("--- Invest. total CE (FBCF × share) ---")
    inv_total = _carregar_invest_total_ce()
    if not inv_total.empty:
        inv_total["ano"] = inv_total["ano"].astype(int)
        inv_total["mes"] = inv_total["mes"].astype(int)
        painel = painel.merge(inv_total, on=["ano", "mes"], how="left")
        # Resíduo privado: invest_total - (estadual + municipal + federal).
        # Atenção: requer harmonização de bases monetárias (ano-base 2010).
        # Implementação completa depende de deflacionamento do SIOF (R$ correntes)
        # e demais fontes públicas — por enquanto deixamos a coluna comentada
        # e o pesquisador computa o residual após decisão de deflator.

    # Persistir
    out_path = MODEL_READY_DIR / "painel_regional_ce_mensal.csv"
    painel = painel.sort_values(["ano", "mes", "regiao_codigo"]).reset_index(drop=True)
    painel.to_csv(out_path, index=False)
    logger.info(
        f"\n=== PAINEL REGIONAL CE ===\n"
        f"  Path: {out_path}\n"
        f"  Linhas: {len(painel)} ({painel['regiao_codigo'].nunique()} regiões × "
        f"{painel.groupby(['ano','mes']).ngroups} meses)\n"
        f"  Colunas: {len(painel.columns)}"
    )
    return painel


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    construir_painel()

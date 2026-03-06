"""
Preparação de dados para o modelo econométrico da tese.

Etapas principais:
  1. Deflacionamento de séries monetárias pelo IPCA
  2. Harmonização temporal para bimestre
  3. Regras explícitas por variável
  4. Construção do painel final model-ready

Uso:
  python3 -m pipeline.transform.preparacao_modelo
"""

from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.config import ESTADOS_NE, RAW_DIR, PROCESSED_DIR
from pipeline.extract.transferencias import TransferenciasConstitucionais


MODEL_READY_DIR = PROCESSED_DIR / "model_ready"
MES_PARA_BIMESTRE = {
    1: 1,
    2: 1,
    3: 2,
    4: 2,
    5: 3,
    6: 3,
    7: 4,
    8: 4,
    9: 5,
    10: 5,
    11: 6,
    12: 6,
}
QUADRIMESTRE_PARA_BIMESTRES = {1: [1, 2], 2: [3, 4], 3: [5, 6]}
UF_NOMES = {uf: info["nome"] for uf, info in ESTADOS_NE.items()}

MODEL_RULES = [
    {
        "variavel": "saldo",
        "fonte": "caged_bimestral",
        "tipo_temporal": "fluxo",
        "regra_bimestral": "soma",
        "deflacionamento": "nao_aplica",
        "unidade_final": "postos_de_trabalho",
    },
    {
        "variavel": "credito_PF_nordeste_real",
        "fonte": "bacen_bimestral",
        "tipo_temporal": "estoque",
        "regra_bimestral": "ultimo_valor",
        "deflacionamento": "ipca",
        "unidade_final": "r$_dez_2025_milhoes",
    },
    {
        "variavel": "credito_PJ_nordeste_real",
        "fonte": "bacen_bimestral",
        "tipo_temporal": "estoque",
        "regra_bimestral": "ultimo_valor",
        "deflacionamento": "ipca",
        "unidade_final": "r$_dez_2025_milhoes",
    },
    {
        "variavel": "resultado_primario_real",
        "fonte": "rreo_resultado_primario",
        "tipo_temporal": "acumulado_no_periodo",
        "regra_bimestral": "valor_reportado",
        "deflacionamento": "ipca",
        "unidade_final": "r$_dez_2025",
    },
    {
        "variavel": "dcl_real",
        "fonte": "rgf_divida",
        "tipo_temporal": "estoque",
        "regra_bimestral": "repete_quadrimestre",
        "deflacionamento": "ipca",
        "unidade_final": "r$_dez_2025",
    },
    {
        "variavel": "investimento_publico_real",
        "fonte": "dca_investimento",
        "tipo_temporal": "anual",
        "regra_bimestral": "repete_ano",
        "deflacionamento": "ipca",
        "unidade_final": "r$_dez_2025",
    },
    {
        "variavel": "transferencias_federais_real",
        "fonte": "transferencias",
        "tipo_temporal": "acumulado_no_ano",
        "regra_bimestral": "diferenca_do_acumulado",
        "deflacionamento": "ipca",
        "unidade_final": "r$_dez_2025",
    },
    {
        "variavel": "rais_vinculos_ativos",
        "fonte": "rais_vinculos",
        "tipo_temporal": "estoque_anual",
        "regra_bimestral": "repete_ano",
        "deflacionamento": "nao_aplica",
        "unidade_final": "vinculos_formais",
    },
]


def _load_first_existing_csv(candidates: list[Path]) -> pd.DataFrame:
    for path in candidates:
        if path.exists():
            return pd.read_csv(path, low_memory=False)
    raise FileNotFoundError(f"Nenhum arquivo encontrado entre: {[str(p) for p in candidates]}")


def _load_many_csvs(paths: list[Path]) -> pd.DataFrame:
    frames = [pd.read_csv(path, low_memory=False) for path in paths if path.exists()]
    if not frames:
        raise FileNotFoundError("Nenhum arquivo encontrado para a etapa solicitada.")
    return pd.concat(frames, ignore_index=True)


def _save_model_ready_csv(df: pd.DataFrame, filename: str) -> Path:
    MODEL_READY_DIR.mkdir(parents=True, exist_ok=True)
    target = MODEL_READY_DIR / f"{filename}.csv"
    df.to_csv(target, index=False, encoding="utf-8-sig")
    return target


def _carregar_ipca() -> pd.Series:
    candidates = [
        RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
    ]
    df = _load_first_existing_csv(candidates)
    df["data"] = pd.to_datetime(df["data"])
    df.sort_values("data", inplace=True)

    ipca_col = next((col for col in df.columns if "ipca" in col.lower()), None)
    if ipca_col is None:
        raise ValueError("Coluna IPCA não encontrada no arquivo BACEN wide.")

    ipca = df.set_index("data")[ipca_col].dropna() / 100 + 1
    indice = ipca.cumprod()
    return (indice / indice.iloc[-1]) * 100


def deflacionar_serie(
    df: pd.DataFrame,
    col_data: str,
    col_valor: str,
    ipca_index: pd.Series | None = None,
    col_saida: str | None = None,
) -> pd.DataFrame:
    if ipca_index is None:
        ipca_index = _carregar_ipca()
    if col_saida is None:
        col_saida = f"{col_valor}_real"

    out = df.copy()
    out[col_data] = pd.to_datetime(out[col_data])
    out["_mes_ref"] = out[col_data].dt.to_period("M").dt.to_timestamp()

    ipca_df = ipca_index.reset_index()
    ipca_df.columns = ["_mes_ref", "_ipca_idx"]
    ipca_df["_mes_ref"] = ipca_df["_mes_ref"].dt.to_period("M").dt.to_timestamp()

    out = out.merge(ipca_df, on="_mes_ref", how="left")
    out[col_saida] = out[col_valor] * (100.0 / out["_ipca_idx"])
    out.drop(columns=["_mes_ref", "_ipca_idx"], inplace=True)
    return out


def _data_referencia_bimestre(ano: pd.Series, bimestre: pd.Series) -> pd.Series:
    meses = pd.to_numeric(bimestre, errors="coerce").astype("Int64") * 2
    return pd.to_datetime(
        {"year": pd.to_numeric(ano, errors="coerce"), "month": meses, "day": 1},
        errors="coerce",
    )


def _data_referencia_ano(ano: pd.Series) -> pd.Series:
    return pd.to_datetime(
        {"year": pd.to_numeric(ano, errors="coerce"), "month": 12, "day": 1},
        errors="coerce",
    )


def deflacionar_bimestral(
    df: pd.DataFrame,
    col_valor: str,
    col_saida: str | None = None,
    col_ano: str = "ano_bim",
    col_bimestre: str = "bimestre",
) -> pd.DataFrame:
    out = df.copy()
    out["_data_ref"] = _data_referencia_bimestre(out[col_ano], out[col_bimestre])
    out = deflacionar_serie(out, "_data_ref", col_valor, col_saida=col_saida)
    return out.drop(columns="_data_ref")


def deflacionar_anual(
    df: pd.DataFrame,
    col_valor: str,
    col_saida: str | None = None,
    col_ano: str = "ano",
) -> pd.DataFrame:
    out = df.copy()
    out["_data_ref"] = _data_referencia_ano(out[col_ano])
    out = deflacionar_serie(out, "_data_ref", col_valor, col_saida=col_saida)
    return out.drop(columns="_data_ref")


def deflacionar_bacen_wide() -> pd.DataFrame:
    candidates = [
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
        RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
    ]
    df = _load_first_existing_csv(candidates)
    df["data"] = pd.to_datetime(df["data"])

    colunas_monetarias = [c for c in df.columns if "credito" in c.lower()]
    for col in colunas_monetarias:
        df = deflacionar_serie(df, "data", col)

    target_dir = PROCESSED_DIR / "bacen" / "nacional"
    target_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(target_dir / "bacen_deflacionado.csv", index=False, encoding="utf-8-sig")
    print(f"    BACEN deflacionado: {len(df)} registros, {len(colunas_monetarias)} séries monetárias.")
    return df


def _atribuir_bimestre(
    df: pd.DataFrame,
    col_data: str | None = None,
    col_ano: str | None = None,
    col_mes: str | None = None,
) -> pd.DataFrame:
    out = df.copy()
    if col_data and col_data in out.columns:
        out[col_data] = pd.to_datetime(out[col_data])
        out["ano_bim"] = out[col_data].dt.year
        out["bimestre"] = out[col_data].dt.month.map(MES_PARA_BIMESTRE)
    elif col_ano and col_mes:
        out["ano_bim"] = pd.to_numeric(out[col_ano], errors="coerce").astype("Int64")
        out["bimestre"] = pd.to_numeric(out[col_mes], errors="coerce").map(MES_PARA_BIMESTRE)
    return out


def agregar_mensal_para_bimestral(
    df: pd.DataFrame,
    col_data: str | None = None,
    col_ano: str | None = None,
    col_mes: str | None = None,
    colunas_soma: list[str] | None = None,
    colunas_media: list[str] | None = None,
    colunas_ultimo: list[str] | None = None,
    colunas_grupo: list[str] | None = None,
) -> pd.DataFrame:
    colunas_soma = colunas_soma or []
    colunas_media = colunas_media or []
    colunas_ultimo = colunas_ultimo or []
    colunas_grupo = colunas_grupo or []

    out = _atribuir_bimestre(df, col_data, col_ano, col_mes)
    grupo = ["ano_bim", "bimestre"] + colunas_grupo

    agg_dict = {}
    for col in colunas_soma:
        if col in out.columns:
            agg_dict[col] = "sum"
    for col in colunas_media:
        if col in out.columns:
            agg_dict[col] = "mean"
    for col in colunas_ultimo:
        if col in out.columns:
            agg_dict[col] = "last"

    if not agg_dict:
        return out.groupby(grupo, as_index=False).first()

    result = out.groupby(grupo, as_index=False).agg(agg_dict)
    result.sort_values(grupo, inplace=True)
    result.reset_index(drop=True, inplace=True)
    return result


def harmonizar_bacen_bimestral() -> pd.DataFrame:
    candidates = [
        PROCESSED_DIR / "bacen" / "nacional" / "bacen_deflacionado.csv",
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
    ]
    df = _load_first_existing_csv(candidates)
    df["data"] = pd.to_datetime(df["data"])

    colunas_taxa = [
        c for c in df.columns if any(k in c.lower() for k in ["selic", "ipca", "inadimplencia"])
    ]
    colunas_estoque = [
        c
        for c in df.columns
        if any(k in c.lower() for k in ["credito", "ibcr", "ibc_br", "pib"])
    ]
    colunas_estoque += [c for c in df.columns if c.endswith("_real")]
    colunas_estoque = sorted(set(colunas_estoque))

    result = agregar_mensal_para_bimestral(
        df,
        col_data="data",
        colunas_media=colunas_taxa,
        colunas_ultimo=colunas_estoque,
    )
    target_dir = PROCESSED_DIR / "bacen" / "nacional"
    target_dir.mkdir(parents=True, exist_ok=True)
    result.to_csv(target_dir / "bacen_bimestral.csv", index=False, encoding="utf-8-sig")
    print(f"    BACEN bimestral: {len(result)} registros.")
    return result


def harmonizar_caged_bimestral() -> pd.DataFrame:
    frames = []
    for arq in ["caged_antigo_saldo_mensal.csv", "caged_saldo_mensal.csv"]:
        path = PROCESSED_DIR / "caged" / "nordeste" / arq
        if not path.exists():
            path = RAW_DIR / "caged" / "nordeste" / arq
        if path.exists():
            frames.append(pd.read_csv(path, low_memory=False))

    if not frames:
        raise FileNotFoundError("CAGED: nenhum arquivo encontrado.")

    df = pd.concat(frames, ignore_index=True)
    df.drop_duplicates(subset=["ano", "mes", "sigla_uf"], keep="last", inplace=True)
    result = agregar_mensal_para_bimestral(
        df,
        col_ano="ano",
        col_mes="mes",
        colunas_soma=["admissoes", "desligamentos", "saldo", "total_movimentacoes"],
        colunas_media=["salario_medio"],
        colunas_grupo=["sigla_uf"],
    )
    target_dir = PROCESSED_DIR / "caged" / "nordeste"
    target_dir.mkdir(parents=True, exist_ok=True)
    result.to_csv(target_dir / "caged_bimestral.csv", index=False, encoding="utf-8-sig")
    print(f"    CAGED bimestral: {len(result)} registros ({result['ano_bim'].min()}-{result['ano_bim'].max()}).")
    return result


def expandir_anual_para_bimestral(
    df: pd.DataFrame,
    col_ano: str,
    colunas_valor: list[str],
    colunas_grupo: list[str],
) -> pd.DataFrame:
    out = df.copy()
    linhas = []
    for _, row in out.iterrows():
        for bimestre in range(1, 7):
            novo = {col: row[col] for col in colunas_grupo + [col_ano] + colunas_valor if col in row.index}
            novo["ano_bim"] = int(row[col_ano])
            novo["bimestre"] = bimestre
            linhas.append(novo)
    return pd.DataFrame(linhas)


def salvar_matriz_regras_modelo() -> pd.DataFrame:
    df = pd.DataFrame(MODEL_RULES)
    _save_model_ready_csv(df, "matriz_regras_modelo")
    return df


def harmonizar_rreo_resultado_primario_bimestral() -> pd.DataFrame:
    paths = sorted((PROCESSED_DIR / "siconfi_rreo").glob("*/rreo_resultado_primario.csv"))
    df = _load_many_csvs(paths)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    out = df[
        (df["cod_conta"] == "ResultadoPrimarioComRPPSAcimaDaLinha")
        & (df["coluna"] == "VALOR")
    ][["exercicio", "periodo", "uf", "valor"]].copy()
    out = out.groupby(["exercicio", "periodo", "uf"], as_index=False)["valor"].sum()
    out.rename(
        columns={
            "exercicio": "ano_bim",
            "periodo": "bimestre",
            "valor": "resultado_primario_nominal",
        },
        inplace=True,
    )
    out = deflacionar_bimestral(out, "resultado_primario_nominal", "resultado_primario_real")
    out.sort_values(["ano_bim", "bimestre", "uf"], inplace=True)
    _save_model_ready_csv(out, "resultado_primario_bimestral")
    return out


def harmonizar_rgf_divida_bimestral() -> pd.DataFrame:
    paths = sorted((PROCESSED_DIR / "siconfi_rgf").glob("*/rgf_divida.csv"))
    df = _load_many_csvs(paths)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df[df["cod_conta"] == "DividaConsolidadaLiquida"].copy()
    quadrimestre_label = {
        1: "1º Quadrimestre",
        2: "2º Quadrimestre",
        3: "3º Quadrimestre",
    }
    df["coluna_match"] = df.apply(
        lambda row: quadrimestre_label.get(int(row["periodo"]), "") in str(row["coluna"]),
        axis=1,
    )
    df = df[df["coluna_match"]][["exercicio", "periodo", "uf", "valor"]].copy()
    df = df.groupby(["exercicio", "periodo", "uf"], as_index=False)["valor"].last()

    linhas = []
    for _, row in df.iterrows():
        for bimestre in QUADRIMESTRE_PARA_BIMESTRES.get(int(row["periodo"]), []):
            linhas.append(
                {
                    "ano_bim": int(row["exercicio"]),
                    "bimestre": bimestre,
                    "uf": row["uf"],
                    "dcl_nominal": row["valor"],
                }
            )
    out = pd.DataFrame(linhas)
    out = deflacionar_bimestral(out, "dcl_nominal", "dcl_real")
    out.sort_values(["ano_bim", "bimestre", "uf"], inplace=True)
    _save_model_ready_csv(out, "dcl_bimestral")
    return out


def harmonizar_dca_investimento_bimestral() -> pd.DataFrame:
    paths = sorted((PROCESSED_DIR / "siconfi_dca").glob("*/dca_investimento.csv"))
    df = _load_many_csvs(paths)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    out = df[df["cod_conta"] == "DO4.4.00.00.00.00"][["exercicio", "uf", "valor"]].copy()
    out = out.groupby(["exercicio", "uf"], as_index=False)["valor"].sum()
    out.rename(columns={"exercicio": "ano", "valor": "investimento_publico_nominal"}, inplace=True)
    out = deflacionar_anual(out, "investimento_publico_nominal", "investimento_publico_real", col_ano="ano")
    out = expandir_anual_para_bimestral(
        out,
        col_ano="ano",
        colunas_valor=["investimento_publico_nominal", "investimento_publico_real"],
        colunas_grupo=["uf"],
    )
    out.sort_values(["ano_bim", "bimestre", "uf"], inplace=True)
    _save_model_ready_csv(out, "investimento_publico_bimestral")
    return out


def harmonizar_transferencias_bimestral() -> pd.DataFrame:
    paths = sorted((PROCESSED_DIR / "transferencias").glob("*/transferencias.csv"))
    df = _load_many_csvs(paths)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df[
        df["cod_conta"].isin(TransferenciasConstitucionais.CONTAS_PERMITIDAS)
        & df["coluna"].str.contains("Até o Bimestre", na=False)
    ][["exercicio", "periodo", "uf", "cod_conta", "valor"]].copy()

    df = df.groupby(["exercicio", "periodo", "uf", "cod_conta"], as_index=False)["valor"].sum()
    df.sort_values(["uf", "cod_conta", "exercicio", "periodo"], inplace=True)
    df["transferencia_bimestral_nominal"] = (
        df.groupby(["uf", "cod_conta", "exercicio"])["valor"].diff().fillna(df["valor"])
    )

    out = df.groupby(["exercicio", "periodo", "uf"], as_index=False)[
        "transferencia_bimestral_nominal"
    ].sum()
    out.rename(
        columns={
            "exercicio": "ano_bim",
            "periodo": "bimestre",
            "transferencia_bimestral_nominal": "transferencias_federais_nominal",
        },
        inplace=True,
    )
    out = deflacionar_bimestral(out, "transferencias_federais_nominal", "transferencias_federais_real")
    out.sort_values(["ano_bim", "bimestre", "uf"], inplace=True)
    _save_model_ready_csv(out, "transferencias_bimestrais")
    return out


def harmonizar_rais_bimestral() -> pd.DataFrame:
    candidates = [PROCESSED_DIR / "rais" / "nordeste" / "rais_vinculos.csv", RAW_DIR / "rais" / "nordeste" / "rais_vinculos.csv"]
    df = _load_first_existing_csv(candidates)
    out = df[["ano", "sigla_uf", "vinculos_ativos", "remuneracao_media"]].copy()
    out.rename(columns={"sigla_uf": "uf", "vinculos_ativos": "rais_vinculos_ativos"}, inplace=True)
    out["remuneracao_media"] = pd.to_numeric(out["remuneracao_media"], errors="coerce")
    out = deflacionar_anual(out, "remuneracao_media", "rais_remuneracao_media_real", col_ano="ano")
    out = expandir_anual_para_bimestral(
        out,
        col_ano="ano",
        colunas_valor=["rais_vinculos_ativos", "remuneracao_media", "rais_remuneracao_media_real"],
        colunas_grupo=["uf"],
    )
    out.sort_values(["ano_bim", "bimestre", "uf"], inplace=True)
    _save_model_ready_csv(out, "rais_bimestral")
    return out


def construir_painel_tese_bimestral() -> pd.DataFrame:
    caged = _load_first_existing_csv([PROCESSED_DIR / "caged" / "nordeste" / "caged_bimestral.csv"])
    bacen = _load_first_existing_csv([PROCESSED_DIR / "bacen" / "nacional" / "bacen_bimestral.csv"])
    resultado_primario = _load_first_existing_csv([MODEL_READY_DIR / "resultado_primario_bimestral.csv"])
    dcl = _load_first_existing_csv([MODEL_READY_DIR / "dcl_bimestral.csv"])
    investimento = _load_first_existing_csv([MODEL_READY_DIR / "investimento_publico_bimestral.csv"])
    transferencias = _load_first_existing_csv([MODEL_READY_DIR / "transferencias_bimestrais.csv"])

    painel = caged.rename(columns={"sigla_uf": "uf"}).copy()
    painel["uf_nome"] = painel["uf"].map(UF_NOMES)
    painel = painel.merge(bacen, on=["ano_bim", "bimestre"], how="left")
    painel = painel.merge(resultado_primario, on=["ano_bim", "bimestre", "uf"], how="left")
    painel = painel.merge(dcl, on=["ano_bim", "bimestre", "uf"], how="left")
    painel = painel.merge(investimento, on=["ano_bim", "bimestre", "uf"], how="left")
    painel = painel.merge(transferencias, on=["ano_bim", "bimestre", "uf"], how="left")

    rais_path = MODEL_READY_DIR / "rais_bimestral.csv"
    if rais_path.exists():
        rais = pd.read_csv(rais_path, low_memory=False)
        painel = painel.merge(
            rais[["ano_bim", "bimestre", "uf", "rais_vinculos_ativos", "rais_remuneracao_media_real"]],
            on=["ano_bim", "bimestre", "uf"],
            how="left",
        )

    ordered_cols = [
        "uf",
        "uf_nome",
        "ano_bim",
        "bimestre",
        "admissoes",
        "desligamentos",
        "saldo",
        "total_movimentacoes",
        "salario_medio",
        "rais_vinculos_ativos",
        "credito_PF_nordeste_real",
        "credito_PJ_nordeste_real",
        "credito_total_nordeste_real",
        "IBCR_NE_ajuste_sazonal",
        "ibc_br",
        "selic_mensal",
        "ipca_mensal",
        "inadimplencia_PF",
        "inadimplencia_PJ",
        "resultado_primario_real",
        "dcl_real",
        "investimento_publico_real",
        "transferencias_federais_real",
        "rais_remuneracao_media_real",
    ]
    ordered_cols = [col for col in ordered_cols if col in painel.columns]
    remaining = [col for col in painel.columns if col not in ordered_cols]
    painel = painel[ordered_cols + remaining]
    painel.sort_values(["uf", "ano_bim", "bimestre"], inplace=True)
    painel.reset_index(drop=True, inplace=True)
    _save_model_ready_csv(painel, "painel_tese_bimestral")
    return painel


def executar_preparacao() -> pd.DataFrame:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    MODEL_READY_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 60)
    print("PREPARAÇÃO DE DADOS PARA O MODELO DA TESE")
    print("=" * 60)

    print("\n--- Etapa 1: Deflacionamento BACEN ---")
    deflacionar_bacen_wide()

    print("\n--- Etapa 2: Harmonização BACEN e CAGED ---")
    harmonizar_bacen_bimestral()
    harmonizar_caged_bimestral()

    print("\n--- Etapa 3: Regras e séries analíticas ---")
    salvar_matriz_regras_modelo()
    harmonizar_rreo_resultado_primario_bimestral()
    harmonizar_rgf_divida_bimestral()
    harmonizar_dca_investimento_bimestral()
    harmonizar_transferencias_bimestral()
    try:
        harmonizar_rais_bimestral()
    except FileNotFoundError:
        print("    RAIS bimestral: arquivo não encontrado, seguindo sem estoque anual.")

    print("\n--- Etapa 4: Painel final ---")
    painel = construir_painel_tese_bimestral()
    print(f"    Painel final: {len(painel)} linhas.")
    print("=" * 60)
    print("Preparação concluída.")
    return painel


if __name__ == "__main__":
    executar_preparacao()

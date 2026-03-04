"""
Preparação de dados para o modelo wavelet.

Duas etapas:
  1. Deflacionamento de séries monetárias pelo IPCA
  2. Harmonização temporal para periodicidade bimestral

Uso:
  python -m pipeline.transform.preparacao_modelo
"""

import pandas as pd
import numpy as np

from pipeline.config import RAW_DIR, PROCESSED_DIR


# =============================================================================
# 1. DEFLACIONAMENTO PELO IPCA
# =============================================================================


def _carregar_ipca() -> pd.Series:
    """
    Carrega IPCA mensal (variação %) e retorna índice acumulado com
    base fixa em dez/2025 = 100.
    """
    candidates = [
        RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
    ]
    df = None
    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            break
    if df is None:
        raise FileNotFoundError("Arquivo BACEN wide não encontrado. Execute a coleta primeiro.")

    df["data"] = pd.to_datetime(df["data"])
    df.sort_values("data", inplace=True)

    ipca_col = None
    for col in df.columns:
        if "ipca" in col.lower():
            ipca_col = col
            break

    if ipca_col is None:
        raise ValueError("Coluna IPCA não encontrada no arquivo BACEN wide.")

    ipca = df.set_index("data")[ipca_col].dropna()
    ipca = ipca / 100 + 1
    indice = ipca.cumprod()

    # Base fixa: último mês disponível = 100
    indice = (indice / indice.iloc[-1]) * 100

    return indice


def deflacionar_serie(
    df: pd.DataFrame,
    col_data: str,
    col_valor: str,
    ipca_index: pd.Series = None,
    col_saida: str = None,
) -> pd.DataFrame:
    """
    Deflaciona uma coluna monetária pelo IPCA.

    Para séries de estoque (saldo de crédito, dívida): usa o índice do mês.
    Para séries de fluxo (transferências, receita): idem, pois são acumulados no período.

    Parâmetros:
        df: DataFrame com a série
        col_data: nome da coluna de data
        col_valor: nome da coluna com valor nominal
        ipca_index: pd.Series com índice IPCA (se None, carrega automaticamente)
        col_saida: nome da coluna deflacionada (default: col_valor + "_real")
    """
    if ipca_index is None:
        ipca_index = _carregar_ipca()

    if col_saida is None:
        col_saida = f"{col_valor}_real"

    out = df.copy()
    out[col_data] = pd.to_datetime(out[col_data])

    # Alinhar por mês (início do mês para merge)
    out["_mes_ref"] = out[col_data].dt.to_period("M").dt.to_timestamp()

    ipca_df = ipca_index.reset_index()
    ipca_df.columns = ["_mes_ref", "_ipca_idx"]
    ipca_df["_mes_ref"] = ipca_df["_mes_ref"].dt.to_period("M").dt.to_timestamp()

    out = out.merge(ipca_df, on="_mes_ref", how="left")

    base_value = 100.0
    out[col_saida] = out[col_valor] * (base_value / out["_ipca_idx"])

    out.drop(columns=["_mes_ref", "_ipca_idx"], inplace=True)
    return out


def deflacionar_bacen_wide() -> pd.DataFrame:
    """Deflaciona todas as séries monetárias do BACEN wide."""
    candidates = [
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
        RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
    ]
    df = None
    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            break
    if df is None:
        raise FileNotFoundError("Arquivo BACEN não encontrado.")

    df["data"] = pd.to_datetime(df["data"])
    ipca_index = _carregar_ipca()

    # Séries monetárias (crédito em R$ milhões)
    colunas_monetarias = [
        c for c in df.columns
        if any(k in c.lower() for k in ["credito", "crédito"])
    ]

    for col in colunas_monetarias:
        df = deflacionar_serie(df, "data", col, ipca_index)

    target_dir = PROCESSED_DIR / "bacen" / "nacional"
    target_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(target_dir / "bacen_deflacionado.csv", index=False, encoding="utf-8-sig")
    print(f"    BACEN deflacionado: {len(df)} registros, {len(colunas_monetarias)} séries deflacionadas.")
    return df


# =============================================================================
# 2. HARMONIZAÇÃO TEMPORAL → BIMESTRAL
# =============================================================================

# Mapeamento mês → bimestre (1-6)
MES_PARA_BIMESTRE = {1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3,
                     7: 4, 8: 4, 9: 5, 10: 5, 11: 6, 12: 6}


def _atribuir_bimestre(df: pd.DataFrame, col_data: str = None,
                       col_ano: str = None, col_mes: str = None) -> pd.DataFrame:
    """Adiciona colunas 'ano_bim' e 'bimestre' ao DataFrame."""
    out = df.copy()
    if col_data and col_data in out.columns:
        out[col_data] = pd.to_datetime(out[col_data])
        out["ano_bim"] = out[col_data].dt.year
        out["bimestre"] = out[col_data].dt.month.map(MES_PARA_BIMESTRE)
    elif col_ano and col_mes:
        out["ano_bim"] = out[col_ano]
        out["bimestre"] = out[col_mes].map(MES_PARA_BIMESTRE)
    return out


def agregar_mensal_para_bimestral(
    df: pd.DataFrame,
    col_data: str = None,
    col_ano: str = None,
    col_mes: str = None,
    colunas_soma: list[str] = None,
    colunas_media: list[str] = None,
    colunas_ultimo: list[str] = None,
    colunas_grupo: list[str] = None,
) -> pd.DataFrame:
    """
    Agrega série mensal para bimestral.

    Regras de agregação:
      - colunas_soma: somadas no bimestre (fluxos: admissões, desligamentos, saldo)
      - colunas_media: média no bimestre (taxas: SELIC, IPCA, inadimplência)
      - colunas_ultimo: último valor do bimestre (estoques: saldo de crédito, IBC-Br)
      - colunas_grupo: colunas de agrupamento adicionais (UF, setor, etc.)
    """
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
    """Converte séries BACEN mensais para bimestrais."""
    candidates = [
        PROCESSED_DIR / "bacen" / "nacional" / "bacen_deflacionado.csv",
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
    ]
    df = None
    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            break
    if df is None:
        raise FileNotFoundError("BACEN processado não encontrado.")

    df["data"] = pd.to_datetime(df["data"])

    # Classificar colunas por tipo de agregação
    colunas_taxa = [c for c in df.columns if any(k in c.lower() for k in [
        "selic", "ipca", "inadimplencia", "inadimplência"
    ])]
    colunas_estoque = [c for c in df.columns if any(k in c.lower() for k in [
        "credito", "crédito", "ibcr", "ibc_br", "pib"
    ])]
    # Incluir versões deflacionadas
    colunas_estoque += [c for c in df.columns if c.endswith("_real")]
    colunas_estoque = list(set(colunas_estoque))

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


def harmonizar_caged_bimestral() -> pd.DataFrame | None:
    """Converte saldo mensal CAGED para bimestral."""
    # Tenta carregar CAGED antigo + novo combinados, ou só o novo
    frames = []
    for arq in ["caged_antigo_saldo_mensal.csv", "caged_saldo_mensal.csv"]:
        path = PROCESSED_DIR / "caged" / "nordeste" / arq
        if not path.exists():
            path = RAW_DIR / "caged" / "nordeste" / arq
        if path.exists():
            frames.append(pd.read_csv(path))

    if not frames:
        print("    CAGED: nenhum arquivo encontrado, pulando.")
        return None

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


def interpolar_anual_para_bimestral(
    df: pd.DataFrame,
    col_ano: str,
    col_valor: str,
    colunas_grupo: list[str] = None,
    metodo: str = "spline",
) -> pd.DataFrame:
    """
    Interpola série anual para bimestral usando spline cúbica (padrão)
    ou linear. Distribui o valor anual em 6 bimestres proporcionalmente.

    Para variáveis de estoque (DCL, investimento): repete o valor anual
    em todos os bimestres (ou interpola se houver variação entre anos).
    """
    colunas_grupo = colunas_grupo or []
    resultados = []

    groups = df.groupby(colunas_grupo) if colunas_grupo else [(None, df)]

    for key, chunk in groups:
        chunk = chunk.sort_values(col_ano).copy()
        anos = chunk[col_ano].values
        valores = pd.to_numeric(chunk[col_valor], errors="coerce").values

        # Expandir para bimestres (6 por ano)
        bimestres_idx = []
        for ano in range(int(anos.min()), int(anos.max()) + 1):
            for bim in range(1, 7):
                bimestres_idx.append((ano, bim))

        bim_df = pd.DataFrame(bimestres_idx, columns=["ano_bim", "bimestre"])

        # Interpolar: ponto de referência = bimestre 6 de cada ano (fim do exercício)
        pontos_ref = pd.DataFrame({
            "ano_bim": anos.astype(int),
            "bimestre": 6,
            col_valor: valores,
        })
        pontos_ref["pos"] = pontos_ref["ano_bim"] + pontos_ref["bimestre"] / 7

        bim_df["pos"] = bim_df["ano_bim"] + bim_df["bimestre"] / 7

        if metodo == "spline" and len(pontos_ref) >= 3:
            from scipy.interpolate import CubicSpline
            cs = CubicSpline(pontos_ref["pos"], pontos_ref[col_valor], extrapolate=True)
            bim_df[col_valor] = cs(bim_df["pos"])
        else:
            bim_df[col_valor] = np.interp(
                bim_df["pos"], pontos_ref["pos"], pontos_ref[col_valor]
            )

        bim_df.drop(columns=["pos"], inplace=True)

        if colunas_grupo and key is not None:
            if isinstance(key, tuple):
                for i, g in enumerate(colunas_grupo):
                    bim_df[g] = key[i]
            else:
                bim_df[colunas_grupo[0]] = key

        resultados.append(bim_df)

    return pd.concat(resultados, ignore_index=True)


# =============================================================================
# ORQUESTRADOR
# =============================================================================


def executar_preparacao():
    """Executa deflacionamento e harmonização temporal completa."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 50)
    print("PREPARAÇÃO DE DADOS PARA MODELO WAVELET")
    print("=" * 50)

    print("\n--- Etapa 1: Deflacionamento ---")
    try:
        deflacionar_bacen_wide()
    except (FileNotFoundError, ValueError) as e:
        print(f"    BACEN deflacionamento: {e}")

    print("\n--- Etapa 2: Harmonização temporal → bimestral ---")
    try:
        harmonizar_bacen_bimestral()
    except FileNotFoundError as e:
        print(f"    BACEN bimestral: {e}")

    harmonizar_caged_bimestral()

    print("=" * 50)
    print("Preparação concluída.")


if __name__ == "__main__":
    executar_preparacao()

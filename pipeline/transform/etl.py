"""
ETL - Processamento de dados brutos para o dashboard Streamlit.

Lê CSVs de dados_nordeste/raw/, agrega e filtra,
salva DataFrames processados como .parquet em dados_nordeste/processed/.
"""

import pandas as pd

from pipeline.config import RAW_DIR, PROCESSED_DIR, UF_NOMES


def processar_bacen():
    print(">>> BACEN...")
    df = pd.read_csv(RAW_DIR / "bacen_sgs_wide.csv")
    df["data"] = pd.to_datetime(df["data"])
    df.sort_values("data", inplace=True)
    df.to_parquet(PROCESSED_DIR / "bacen.parquet")
    print(f"    {len(df)} registros -> bacen.parquet")


def processar_bolsa_familia():
    print(">>> Bolsa Família...")
    path = RAW_DIR / "bolsa_familia_capitais_ne.csv"
    if not path.exists():
        print("    Arquivo não encontrado, pulando.")
        return
    df = pd.read_csv(path)
    df["data"] = pd.to_datetime(
        df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )
    df.sort_values(["data", "uf"], inplace=True)
    df.to_parquet(PROCESSED_DIR / "bolsa_familia.parquet")
    print(f"    {len(df)} registros -> bolsa_familia.parquet")


def processar_rreo():
    print(">>> SICONFI RREO...")
    df = pd.read_csv(RAW_DIR / "siconfi_rreo_nordeste.csv", low_memory=False)

    contas_chave = [
        "ReceitasExcetoIntraOrcamentarias",
        "ReceitasCorrentes",
        "ReceitaTributaria",
        "TransferenciasCorrentes",
        "ReceitasDeCapital",
        "DespesasExcetoIntraOrcamentarias",
        "DespesasCorrentes",
        "PessoalEEncargos",
        "JurosEEncargos",
        "OutrasDespesasCorrentes",
        "DespesasDeCapital",
        "Investimentos",
    ]

    mask = (
        df["cod_conta"].isin(contas_chave)
        & df["coluna"].str.contains("Até o Bimestre", na=False)
        & (df["anexo"] == "RREO-Anexo 01")
    )
    out = df.loc[
        mask,
        ["exercicio", "periodo", "uf", "cod_conta", "conta", "valor", "populacao"],
    ].copy()
    out["valor"] = pd.to_numeric(out["valor"], errors="coerce")
    out["uf_nome"] = out["uf"].map(UF_NOMES)
    out.sort_values(["exercicio", "periodo", "uf"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    out.to_parquet(PROCESSED_DIR / "rreo_resumo.parquet")
    print(f"    {len(df)} -> {len(out)} registros -> rreo_resumo.parquet")


def processar_rgf():
    print(">>> SICONFI RGF...")
    df = pd.read_csv(RAW_DIR / "siconfi_rgf_nordeste.csv", low_memory=False)

    contas_chave = [
        "DespesaComPessoalBruta",
        "DespesaComPessoalLiquida",
        "DespesaComPessoalAtivoBruta",
        "DespesaComPessoalInativoEPensionistasBruta",
        "ReceitaCorrenteLiquidaLimiteLegal",
        "DespesaComPessoalTotal",
        "DividaConsolidada",
        "DividaConsolidadaLiquida",
        "DividaContratual",
    ]

    mask = df["cod_conta"].isin(contas_chave) & (
        df["anexo"].isin(["RGF-Anexo 01", "RGF-Anexo 02"])
    )
    out = df.loc[
        mask,
        [
            "exercicio",
            "periodo",
            "uf",
            "anexo",
            "cod_conta",
            "conta",
            "coluna",
            "valor",
            "populacao",
        ],
    ].copy()
    out["valor"] = pd.to_numeric(out["valor"], errors="coerce")
    out["uf_nome"] = out["uf"].map(UF_NOMES)
    out.sort_values(["exercicio", "periodo", "uf"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    out.to_parquet(PROCESSED_DIR / "rgf_resumo.parquet")
    print(f"    {len(df)} -> {len(out)} registros -> rgf_resumo.parquet")


def processar_dca():
    print(">>> SICONFI DCA...")
    df = pd.read_csv(RAW_DIR / "siconfi_dca_nordeste.csv", low_memory=False)

    contas_chave = {
        "P1.0.0.0.0.00.00": "Ativo Total",
        "P1.1.0.0.0.00.00": "Ativo Circulante",
        "P1.2.0.0.0.00.00": "Ativo Não Circulante",
        "P2.0.0.0.0.00.00": "Passivo Total",
        "P2.1.0.0.0.00.00": "Passivo Circulante",
        "P2.2.0.0.0.00.00": "Passivo Não Circulante",
        "P2.3.0.0.0.00.00": "Patrimônio Líquido",
    }

    mask = df["cod_conta"].isin(contas_chave.keys()) & (
        df["anexo"] == "DCA-Anexo I-AB"
    )
    out = df.loc[
        mask, ["exercicio", "uf", "cod_conta", "conta", "valor", "populacao"]
    ].copy()
    out["valor"] = pd.to_numeric(out["valor"], errors="coerce")
    out["conta_resumo"] = out["cod_conta"].map(contas_chave)
    out["uf_nome"] = out["uf"].map(UF_NOMES)
    out.sort_values(["exercicio", "uf"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    out.to_parquet(PROCESSED_DIR / "dca_resumo.parquet")
    print(f"    {len(df)} -> {len(out)} registros -> dca_resumo.parquet")


def processar_transferencias():
    print(">>> Transferências Constitucionais...")
    df = pd.read_csv(
        RAW_DIR / "transferencias_constitucionais_nordeste.csv", low_memory=False
    )
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    mask = df["coluna"].str.contains("Até o Bimestre", na=False)
    out = df.loc[mask].copy()

    out["uf_nome"] = out["uf"].map(UF_NOMES)
    out.sort_values(["exercicio", "periodo", "uf"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    out.to_parquet(PROCESSED_DIR / "transferencias.parquet")
    print(f"    {len(df)} -> {len(out)} registros -> transferencias.parquet")


def processar_siof():
    print(">>> SIOF-CE...")
    path = RAW_DIR / "siof_consolidado.parquet"
    if not path.exists():
        # Fallback para CSV
        path_csv = RAW_DIR / "siof_consolidado.csv"
        if not path_csv.exists():
            print("    Arquivo não encontrado, pulando.")
            return
        df = pd.read_csv(path_csv)
    else:
        df = pd.read_parquet(path)

    # Garantir tipos numéricos nas colunas de valor
    for col in ["Lei", "Lei + Cred.", "Empenhado", "Pago", "% Emp.", "% Pago"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.sort_values(["ano", "Descrição"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_parquet(PROCESSED_DIR / "siof_ce.parquet")
    print(f"    {len(df)} registros -> siof_ce.parquet")


def executar_etl():
    """Executa todas as etapas do ETL."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    print("=" * 50)
    print("ETL - Processamento de dados brutos")
    print("=" * 50)
    processar_bacen()
    processar_bolsa_familia()
    processar_rreo()
    processar_rgf()
    processar_dca()
    processar_transferencias()
    processar_siof()
    print("=" * 50)
    print("ETL concluído.")


if __name__ == "__main__":
    executar_etl()

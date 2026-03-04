"""
ETL - Processamento de dados brutos para o dashboard Streamlit.

Lê CSVs de dados_nordeste/raw/, agrega e filtra,
salva DataFrames processados como CSV em dados_nordeste/processed/.
"""

import pandas as pd

from pipeline.config import RAW_DIR, PROCESSED_DIR, UF_NOMES


def _read_csv_candidates(candidates: list[str], **kwargs) -> pd.DataFrame:
    """Lê o primeiro CSV existente entre caminhos candidatos relativos a RAW_DIR."""
    for rel_path in candidates:
        path = RAW_DIR / rel_path
        if path.exists():
            return pd.read_csv(path, **kwargs)
    raise FileNotFoundError(f"Nenhum arquivo encontrado em RAW_DIR para: {candidates}")


def _save_processed_csv(df: pd.DataFrame, orgao: str, estado: str, nome_arquivo: str):
    """Salva CSV processado em estrutura processed/<orgao>/<estado>/."""
    target_dir = PROCESSED_DIR / orgao / estado
    target_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(target_dir / f"{nome_arquivo}.csv", index=False, encoding="utf-8-sig")


def _adicionar_colunas_similares_siof(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona aliases no padrão do SIOF para facilitar comparação entre estados.
    Mantém as colunas originais para compatibilidade.
    """
    out = df.copy()

    if "cod_ug" in out.columns and "Código" not in out.columns:
        out["Código"] = out["cod_ug"]
    if "unidade_gestora" in out.columns and "Descrição" not in out.columns:
        out["Descrição"] = out["unidade_gestora"]
    if "dotacao_inicial" in out.columns and "Lei" not in out.columns:
        out["Lei"] = pd.to_numeric(out["dotacao_inicial"], errors="coerce")
    if "dotacao_atualizada" in out.columns and "Lei + Cred." not in out.columns:
        out["Lei + Cred."] = pd.to_numeric(out["dotacao_atualizada"], errors="coerce")
    if "empenhado" in out.columns and "Empenhado" not in out.columns:
        out["Empenhado"] = pd.to_numeric(out["empenhado"], errors="coerce")
    if "liquidado" in out.columns and "Liquidado" not in out.columns:
        out["Liquidado"] = pd.to_numeric(out["liquidado"], errors="coerce")
    if "pago" in out.columns and "Pago" not in out.columns:
        out["Pago"] = pd.to_numeric(out["pago"], errors="coerce")

    if "Lei + Cred." in out.columns:
        base = pd.to_numeric(out["Lei + Cred."], errors="coerce")
        base = base.where(base > 0)
        if "Empenhado" in out.columns and "% Emp." not in out.columns:
            out["% Emp."] = (out["Empenhado"] / base) * 100
        if "Pago" in out.columns and "% Pago" not in out.columns:
            out["% Pago"] = (out["Pago"] / base) * 100

    return out


def processar_bacen():
    print(">>> BACEN...")
    df = _read_csv_candidates(
        ["bacen/nacional/bacen_sgs_wide.csv", "bacen_sgs_wide.csv"]
    )
    df["data"] = pd.to_datetime(df["data"])
    df.sort_values("data", inplace=True)
    _save_processed_csv(df, "bacen", "nacional", "bacen")
    print(f"    {len(df)} registros -> processed/bacen/nacional/bacen.csv")


def processar_bolsa_familia():
    print(">>> Bolsa Família...")
    try:
        df = _read_csv_candidates(
            [
                "bolsa_familia/nordeste/bolsa_familia_capitais_ne.csv",
                "bolsa_familia_capitais_ne.csv",
            ]
        )
    except FileNotFoundError:
        print("    Arquivo não encontrado, pulando.")
        return

    df["data"] = pd.to_datetime(
        df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
    )
    df.sort_values(["data", "uf"], inplace=True)
    if "uf" in df.columns:
        for uf, chunk in df.groupby("uf"):
            _save_processed_csv(chunk, "bolsa_familia", str(uf).lower(), "bolsa_familia")
        print("    CSVs por UF salvos em processed/bolsa_familia/<uf>/bolsa_familia.csv")
    else:
        _save_processed_csv(df, "bolsa_familia", "nordeste", "bolsa_familia")
        print("    CSV salvo em processed/bolsa_familia/nordeste/bolsa_familia.csv")


def processar_rreo():
    print(">>> SICONFI RREO...")
    df = _read_csv_candidates(
        ["siconfi/nordeste/siconfi_rreo_nordeste.csv", "siconfi_rreo_nordeste.csv"],
        low_memory=False,
    )

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
    for uf, chunk in out.groupby("uf"):
        _save_processed_csv(chunk, "siconfi_rreo", str(uf).lower(), "rreo_resumo")
    print(f"    {len(df)} -> {len(out)} registros -> processed/siconfi_rreo/<uf>/rreo_resumo.csv")

    # --- Resultado Primário (Anexo 06) ---
    contas_rp = [
        "ResultadoPrimarioComRPPSAcimaDaLinha",
        "ResultadoPrimarioSemRPPSAcimaDaLinha",
        "ReceitaPrimariaTotal",
        "DespesaPrimariaTotal",
    ]
    mask_rp = (
        df["cod_conta"].isin(contas_rp)
        & (df["anexo"] == "RREO-Anexo 06")
    )
    out_rp = df.loc[
        mask_rp,
        ["exercicio", "periodo", "uf", "cod_conta", "conta", "coluna", "valor", "populacao"],
    ].copy()
    out_rp["valor"] = pd.to_numeric(out_rp["valor"], errors="coerce")
    out_rp["uf_nome"] = out_rp["uf"].map(UF_NOMES)
    out_rp.sort_values(["exercicio", "periodo", "uf"], inplace=True)
    out_rp.reset_index(drop=True, inplace=True)
    for uf, chunk in out_rp.groupby("uf"):
        _save_processed_csv(chunk, "siconfi_rreo", str(uf).lower(), "rreo_resultado_primario")
    print(f"    Resultado Primário: {len(out_rp)} registros -> processed/siconfi_rreo/<uf>/rreo_resultado_primario.csv")


def processar_rgf():
    print(">>> SICONFI RGF...")
    df = _read_csv_candidates(
        ["siconfi/nordeste/siconfi_rgf_nordeste.csv", "siconfi_rgf_nordeste.csv"],
        low_memory=False,
    )

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
    for uf, chunk in out.groupby("uf"):
        _save_processed_csv(chunk, "siconfi_rgf", str(uf).lower(), "rgf_resumo")
    print(f"    {len(df)} -> {len(out)} registros -> processed/siconfi_rgf/<uf>/rgf_resumo.csv")

    # --- DCL isolada (variável do modelo wavelet) ---
    contas_dcl = ["DividaConsolidada", "DividaConsolidadaLiquida", "DividaContratual"]
    mask_dcl = df["cod_conta"].isin(contas_dcl) & (df["anexo"] == "RGF-Anexo 02")
    out_dcl = df.loc[
        mask_dcl,
        ["exercicio", "periodo", "uf", "cod_conta", "conta", "coluna", "valor", "populacao"],
    ].copy()
    out_dcl["valor"] = pd.to_numeric(out_dcl["valor"], errors="coerce")
    out_dcl["uf_nome"] = out_dcl["uf"].map(UF_NOMES)
    out_dcl.sort_values(["exercicio", "periodo", "uf"], inplace=True)
    out_dcl.reset_index(drop=True, inplace=True)
    for uf, chunk in out_dcl.groupby("uf"):
        _save_processed_csv(chunk, "siconfi_rgf", str(uf).lower(), "rgf_divida")
    print(f"    DCL: {len(out_dcl)} registros -> processed/siconfi_rgf/<uf>/rgf_divida.csv")


def processar_dca():
    print(">>> SICONFI DCA...")
    df = _read_csv_candidates(
        ["siconfi/nordeste/siconfi_dca_nordeste.csv", "siconfi_dca_nordeste.csv"],
        low_memory=False,
    )

    # --- Balanço patrimonial (Anexo I-AB) ---
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
    for uf, chunk in out.groupby("uf"):
        _save_processed_csv(chunk, "siconfi_dca", str(uf).lower(), "dca_resumo")
    print(f"    {len(df)} -> {len(out)} registros -> processed/siconfi_dca/<uf>/dca_resumo.csv")

    # --- Investimento público (Anexo I-D: Despesas por categoria econômica) ---
    contas_investimento = {
        "DO4.4.00.00.00.00": "Investimentos (Despesas de Capital)",
        "DO4.0.00.00.00.00": "Total Despesas",
        "DO4.3.00.00.00.00": "Despesas Correntes",
    }
    mask_inv = df["cod_conta"].isin(contas_investimento.keys()) & (
        df["anexo"] == "DCA-Anexo I-D"
    )
    colunas_disp = [c for c in ["exercicio", "uf", "cod_conta", "conta", "coluna", "valor", "populacao"] if c in df.columns]
    out_inv = df.loc[mask_inv, colunas_disp].copy()
    out_inv["valor"] = pd.to_numeric(out_inv["valor"], errors="coerce")
    out_inv["conta_resumo"] = out_inv["cod_conta"].map(contas_investimento)
    out_inv["uf_nome"] = out_inv["uf"].map(UF_NOMES)

    if "coluna" in out_inv.columns:
        out_inv = out_inv[out_inv["coluna"].str.contains("Liquidadas", na=False)]

    out_inv.sort_values(["exercicio", "uf"], inplace=True)
    out_inv.reset_index(drop=True, inplace=True)
    for uf, chunk in out_inv.groupby("uf"):
        _save_processed_csv(chunk, "siconfi_dca", str(uf).lower(), "dca_investimento")
    print(f"    Investimento público: {len(out_inv)} registros -> processed/siconfi_dca/<uf>/dca_investimento.csv")


def processar_transferencias():
    print(">>> Transferências Constitucionais...")
    df = _read_csv_candidates(
        [
            "transferencias/nordeste/transferencias_constitucionais_nordeste.csv",
            "transferencias_constitucionais_nordeste.csv",
        ],
        low_memory=False,
    )
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    mask = df["coluna"].str.contains("Até o Bimestre", na=False)
    out = df.loc[mask].copy()

    out["uf_nome"] = out["uf"].map(UF_NOMES)
    out.sort_values(["exercicio", "periodo", "uf"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    for uf, chunk in out.groupby("uf"):
        _save_processed_csv(chunk, "transferencias", str(uf).lower(), "transferencias")
    print(f"    {len(df)} -> {len(out)} registros -> processed/transferencias/<uf>/transferencias.csv")


def processar_transparencia_al():
    print(">>> Transparência AL...")
    try:
        df = _read_csv_candidates(
            [
                "execucao_orcamentaria/al/transparencia_al_consolidado.csv",
                "transparencia/al/transparencia_al_consolidado.csv",
                "transparencia_al_consolidado.csv",
            ]
        )
    except FileNotFoundError:
        print("    Arquivo não encontrado, pulando.")
        return

    for col in ["dotacao_inicial", "dotacao_atualizada", "empenhado", "liquidado", "pago"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    df = _adicionar_colunas_similares_siof(df)
    df.sort_values(["ano", "unidade_gestora"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    _save_processed_csv(df, "execucao_orcamentaria", "al", "transparencia_al")
    print(f"    {len(df)} registros -> processed/execucao_orcamentaria/al/transparencia_al.csv")


def processar_siof():
    print(">>> SIOF-CE...")
    try:
        df = _read_csv_candidates(
            [
                "execucao_orcamentaria/ce/siof_consolidado.csv",
                "siof/ce/siof_consolidado.csv",
                "siof_consolidado.csv",
            ]
        )
    except FileNotFoundError:
        print("    Arquivo não encontrado, pulando.")
        return

    # Garantir tipos numéricos nas colunas de valor
    for col in ["Lei", "Lei + Cred.", "Empenhado", "Pago", "% Emp.", "% Pago"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.sort_values(["ano", "Descrição"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    _save_processed_csv(df, "execucao_orcamentaria", "ce", "siof_ce")
    print(f"    {len(df)} registros -> processed/execucao_orcamentaria/ce/siof_ce.csv")


def processar_transparencia_pi():
    print(">>> Transparencia PI...")
    try:
        df = _read_csv_candidates(
            [
                "execucao_orcamentaria/pi/transparencia_pi_consolidado.csv",
                "transparencia/pi/transparencia_pi_consolidado.csv",
                "transparencia_pi_consolidado.csv",
            ]
        )
    except FileNotFoundError:
        print("    Arquivo não encontrado, pulando.")
        return

    for col in ["dotacao_inicial", "dotacao_atualizada", "empenhado", "liquidado", "pago"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    df = _adicionar_colunas_similares_siof(df)
    df.sort_values(["ano", "unidade_gestora"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    _save_processed_csv(df, "execucao_orcamentaria", "pi", "transparencia_pi")
    print(f"    {len(df)} registros -> processed/execucao_orcamentaria/pi/transparencia_pi.csv")


def processar_caged_antigo():
    print(">>> CAGED Antigo (2015–2019)...")

    try:
        df_saldo = _read_csv_candidates(
            ["caged/nordeste/caged_antigo_saldo_mensal.csv"]
        )
    except FileNotFoundError:
        print("    CAGED Antigo saldo mensal não encontrado, pulando.")
        return

    for col in ["admissoes", "desligamentos", "saldo", "total_movimentacoes"]:
        if col in df_saldo.columns:
            df_saldo[col] = pd.to_numeric(df_saldo[col], errors="coerce").astype("Int64")
    if "salario_medio" in df_saldo.columns:
        df_saldo["salario_medio"] = pd.to_numeric(df_saldo["salario_medio"], errors="coerce")
    df_saldo["uf_nome"] = df_saldo["sigla_uf"].map(UF_NOMES)
    df_saldo.sort_values(["ano", "mes", "sigla_uf"], inplace=True)
    df_saldo.reset_index(drop=True, inplace=True)
    for uf, chunk in df_saldo.groupby("sigla_uf"):
        _save_processed_csv(chunk, "caged", str(uf).lower(), "caged_antigo_saldo_mensal")
    _save_processed_csv(df_saldo, "caged", "nordeste", "caged_antigo_saldo_mensal")
    print(f"    CAGED Antigo saldo mensal: {len(df_saldo)} registros")

    try:
        df_setor = _read_csv_candidates(
            ["caged/nordeste/caged_antigo_por_setor.csv"]
        )
        df_setor["saldo"] = pd.to_numeric(df_setor["saldo"], errors="coerce").astype("Int64")
        if "salario_medio" in df_setor.columns:
            df_setor["salario_medio"] = pd.to_numeric(df_setor["salario_medio"], errors="coerce")
        df_setor["uf_nome"] = df_setor["sigla_uf"].map(UF_NOMES)
        _save_processed_csv(df_setor, "caged", "nordeste", "caged_antigo_por_setor")
        print(f"    CAGED Antigo por setor: {len(df_setor)} registros")
    except FileNotFoundError:
        print("    CAGED Antigo dados por setor não encontrados.")

    try:
        df_perfil = _read_csv_candidates(
            ["caged/nordeste/caged_antigo_por_perfil.csv"]
        )
        for col in ["admissoes", "desligamentos", "saldo"]:
            if col in df_perfil.columns:
                df_perfil[col] = pd.to_numeric(df_perfil[col], errors="coerce").astype("Int64")
        if "salario_medio" in df_perfil.columns:
            df_perfil["salario_medio"] = pd.to_numeric(df_perfil["salario_medio"], errors="coerce")
        df_perfil["uf_nome"] = df_perfil["sigla_uf"].map(UF_NOMES)
        _save_processed_csv(df_perfil, "caged", "nordeste", "caged_antigo_por_perfil")
        print(f"    CAGED Antigo por perfil: {len(df_perfil)} registros")
    except FileNotFoundError:
        print("    CAGED Antigo dados por perfil não encontrados.")


def processar_caged():
    print(">>> CAGED Novo (2020+)...")

    # --- Saldo mensal ---
    try:
        df_saldo = _read_csv_candidates(
            ["caged/nordeste/caged_saldo_mensal.csv"]
        )
    except FileNotFoundError:
        print("    Saldo mensal não encontrado, pulando.")
        return

    df_saldo["salario_medio"] = pd.to_numeric(df_saldo["salario_medio"], errors="coerce")
    for col in ["admissoes", "desligamentos", "saldo", "total_movimentacoes"]:
        if col in df_saldo.columns:
            df_saldo[col] = pd.to_numeric(df_saldo[col], errors="coerce").astype("Int64")
    df_saldo["uf_nome"] = df_saldo["sigla_uf"].map(UF_NOMES)
    df_saldo.sort_values(["ano", "mes", "sigla_uf"], inplace=True)
    df_saldo.reset_index(drop=True, inplace=True)
    for uf, chunk in df_saldo.groupby("sigla_uf"):
        _save_processed_csv(chunk, "caged", str(uf).lower(), "caged_saldo_mensal")
    _save_processed_csv(df_saldo, "caged", "nordeste", "caged_saldo_mensal")
    print(f"    Saldo mensal: {len(df_saldo)} registros")

    # --- Por setor ---
    try:
        df_setor = _read_csv_candidates(
            ["caged/nordeste/caged_por_setor.csv"]
        )
        df_setor["salario_medio"] = pd.to_numeric(df_setor["salario_medio"], errors="coerce")
        df_setor["saldo"] = pd.to_numeric(df_setor["saldo"], errors="coerce").astype("Int64")
        df_setor["uf_nome"] = df_setor["sigla_uf"].map(UF_NOMES)
        _save_processed_csv(df_setor, "caged", "nordeste", "caged_por_setor")
        print(f"    Por setor: {len(df_setor)} registros")
    except FileNotFoundError:
        print("    Dados por setor não encontrados.")

    # --- Por perfil ---
    try:
        df_perfil = _read_csv_candidates(
            ["caged/nordeste/caged_por_perfil.csv"]
        )
        df_perfil["salario_medio"] = pd.to_numeric(df_perfil["salario_medio"], errors="coerce")
        for col in ["admissoes", "desligamentos", "saldo"]:
            if col in df_perfil.columns:
                df_perfil[col] = pd.to_numeric(df_perfil[col], errors="coerce").astype("Int64")
        df_perfil["uf_nome"] = df_perfil["sigla_uf"].map(UF_NOMES)
        _save_processed_csv(df_perfil, "caged", "nordeste", "caged_por_perfil")
        print(f"    Por perfil: {len(df_perfil)} registros")
    except FileNotFoundError:
        print("    Dados por perfil não encontrados.")


def processar_rais():
    print(">>> RAIS (vínculos formais)...")

    # --- Vínculos por UF ---
    try:
        df_vinculos = _read_csv_candidates(
            ["rais/nordeste/rais_vinculos.csv"]
        )
    except FileNotFoundError:
        print("    Vínculos não encontrados, pulando.")
        return

    df_vinculos["remuneracao_media"] = pd.to_numeric(df_vinculos["remuneracao_media"], errors="coerce")
    df_vinculos["vinculos_ativos"] = pd.to_numeric(df_vinculos["vinculos_ativos"], errors="coerce").astype("Int64")
    df_vinculos["uf_nome"] = df_vinculos["sigla_uf"].map(UF_NOMES)
    df_vinculos.sort_values(["ano", "sigla_uf"], inplace=True)
    df_vinculos.reset_index(drop=True, inplace=True)
    for uf, chunk in df_vinculos.groupby("sigla_uf"):
        _save_processed_csv(chunk, "rais", str(uf).lower(), "rais_vinculos")
    _save_processed_csv(df_vinculos, "rais", "nordeste", "rais_vinculos")
    print(f"    Vínculos: {len(df_vinculos)} registros")

    # --- Por setor ---
    try:
        df_setor = _read_csv_candidates(
            ["rais/nordeste/rais_por_setor.csv"]
        )
        df_setor["remuneracao_media"] = pd.to_numeric(df_setor["remuneracao_media"], errors="coerce")
        df_setor["vinculos_ativos"] = pd.to_numeric(df_setor["vinculos_ativos"], errors="coerce").astype("Int64")
        df_setor["uf_nome"] = df_setor["sigla_uf"].map(UF_NOMES)
        _save_processed_csv(df_setor, "rais", "nordeste", "rais_por_setor")
        print(f"    Por setor: {len(df_setor)} registros")
    except FileNotFoundError:
        print("    Dados por setor não encontrados.")


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
    processar_transparencia_al()
    processar_transparencia_pi()
    processar_siof()
    processar_caged_antigo()
    processar_caged()
    processar_rais()
    print("=" * 50)
    print("ETL concluído.")


if __name__ == "__main__":
    executar_etl()

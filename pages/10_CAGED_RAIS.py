import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from pipeline.config import PROCESSED_DIR, UF_NOMES

st.set_page_config(page_title="CAGED / RAIS", layout="wide")
st.title("CAGED / RAIS — Mercado de Trabalho Formal no Nordeste")

PROC = PROCESSED_DIR

# ============================================================================
# Carregamento de dados
# ============================================================================

caged_saldo_path = PROC / "caged" / "nordeste" / "caged_saldo_mensal.csv"
caged_setor_path = PROC / "caged" / "nordeste" / "caged_por_setor.csv"
caged_perfil_path = PROC / "caged" / "nordeste" / "caged_por_perfil.csv"
rais_vinculos_path = PROC / "rais" / "nordeste" / "rais_vinculos.csv"
rais_setor_path = PROC / "rais" / "nordeste" / "rais_por_setor.csv"

tem_caged = caged_saldo_path.exists()
tem_rais = rais_vinculos_path.exists()

if not tem_caged and not tem_rais:
    st.warning(
        "Dados CAGED/RAIS nao encontrados. Execute a coleta e o ETL:\n\n"
        "```bash\n"
        "python -m pipeline.run --modulos caged_rais\n"
        "python -m pipeline.transform.etl\n"
        "```"
    )
    st.stop()

# ============================================================================
# Abas
# ============================================================================

tab_caged, tab_rais = st.tabs(["CAGED — Emprego Formal", "RAIS — Vinculos Formais"])

# ============================================================================
# ABA CAGED
# ============================================================================

with tab_caged:
    if not tem_caged:
        st.info("Dados do CAGED nao encontrados. Execute a coleta.")
        st.stop()

    df_saldo = pd.read_csv(caged_saldo_path)
    if "uf_nome" not in df_saldo.columns:
        df_saldo["uf_nome"] = df_saldo["sigla_uf"].map(UF_NOMES)

    # --- Filtros ---
    col1, col2 = st.columns(2)
    with col1:
        anos = sorted(df_saldo["ano"].unique())
        sel_anos = st.slider(
            "Periodo",
            int(min(anos)), int(max(anos)),
            (int(min(anos)), int(max(anos))),
            key="caged_ano",
        )
    with col2:
        ufs = sorted(df_saldo["sigla_uf"].unique())
        sel_ufs = st.multiselect(
            "Estados", ufs, default=ufs, key="caged_uf",
        )

    mask = df_saldo["ano"].between(*sel_anos) & df_saldo["sigla_uf"].isin(sel_ufs)
    filt = df_saldo[mask]

    # --- Metricas gerais ---
    if not filt.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Admissoes", f"{filt['admissoes'].sum():,.0f}")
        c2.metric("Desligamentos", f"{filt['desligamentos'].sum():,.0f}")
        saldo_total = filt["saldo"].sum()
        c3.metric("Saldo Liquido", f"{saldo_total:+,.0f}")
        c4.metric("Salario Medio", f"R$ {filt['salario_medio'].mean():,.2f}")

    st.divider()

    # --- Evolucao mensal do saldo ---
    st.subheader("Evolucao Mensal do Saldo de Emprego")
    saldo_mensal = filt.groupby(["ano", "mes"], as_index=False)["saldo"].sum()
    saldo_mensal["data"] = pd.to_datetime(
        saldo_mensal["ano"].astype(str) + "-" + saldo_mensal["mes"].astype(str).str.zfill(2) + "-01"
    )
    saldo_mensal.sort_values("data", inplace=True)

    fig = go.Figure()
    colors = saldo_mensal["saldo"].apply(lambda x: "#2ecc71" if x >= 0 else "#e74c3c")
    fig.add_trace(go.Bar(
        x=saldo_mensal["data"], y=saldo_mensal["saldo"],
        marker_color=colors, name="Saldo",
    ))
    fig.update_layout(
        hovermode="x unified", height=400, margin=dict(t=10),
        yaxis_title="Saldo de Empregos", xaxis_title="",
        yaxis_tickformat=",d",
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Comparacao entre estados ---
    st.subheader("Saldo Acumulado por Estado")
    saldo_uf = filt.groupby("sigla_uf", as_index=False).agg(
        saldo=("saldo", "sum"),
        admissoes=("admissoes", "sum"),
        desligamentos=("desligamentos", "sum"),
        salario_medio=("salario_medio", "mean"),
    )
    saldo_uf["uf_nome"] = saldo_uf["sigla_uf"].map(UF_NOMES)
    saldo_uf.sort_values("saldo", ascending=True, inplace=True)

    fig = px.bar(
        saldo_uf, x="saldo", y="uf_nome", orientation="h",
        labels={"saldo": "Saldo de Empregos", "uf_nome": ""},
        text_auto=",.0f",
        color="saldo",
        color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
    )
    fig.update_layout(
        height=400, margin=dict(t=10, l=200),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Evolucao por UF (linhas) ---
    st.subheader("Evolucao Anual por Estado")
    saldo_anual_uf = filt.groupby(["ano", "sigla_uf"], as_index=False)["saldo"].sum()
    saldo_anual_uf["uf_nome"] = saldo_anual_uf["sigla_uf"].map(UF_NOMES)

    fig = px.line(
        saldo_anual_uf, x="ano", y="saldo", color="uf_nome",
        labels={"saldo": "Saldo Anual", "ano": "Ano", "uf_nome": "Estado"},
        markers=True,
    )
    fig.update_layout(
        hovermode="x unified", height=400, margin=dict(t=10),
        yaxis_tickformat=",d", xaxis_dtick=1,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Analise setorial ---
    if caged_setor_path.exists():
        st.subheader("Saldo por Setor Economico (Divisao CNAE)")
        df_setor = pd.read_csv(caged_setor_path)
        mask_s = df_setor["ano"].between(*sel_anos) & df_setor["sigla_uf"].isin(sel_ufs)
        setor_filt = df_setor[mask_s]
        if not setor_filt.empty:
            setor_agg = setor_filt.groupby("divisao_cnae", as_index=False).agg(
                saldo=("saldo", "sum"),
                salario_medio=("salario_medio", "mean"),
            )
            setor_agg.sort_values("saldo", ascending=True, inplace=True)
            top_setores = pd.concat([setor_agg.head(10), setor_agg.tail(10)]).drop_duplicates()
            top_setores.sort_values("saldo", ascending=True, inplace=True)

            fig = px.bar(
                top_setores, x="saldo", y="divisao_cnae", orientation="h",
                labels={"saldo": "Saldo", "divisao_cnae": "Divisao CNAE"},
                text_auto=",.0f",
                color="saldo",
                color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
            )
            fig.update_layout(
                height=max(400, len(top_setores) * 30), margin=dict(t=10, l=150),
                coloraxis_showscale=False,
                yaxis_type="category",
            )
            st.plotly_chart(fig, use_container_width=True)

    # --- Analise demografica ---
    if caged_perfil_path.exists():
        st.subheader("Saldo por Perfil Demografico")
        df_perfil = pd.read_csv(caged_perfil_path)
        mask_p = df_perfil["ano"].between(*sel_anos) & df_perfil["sigla_uf"].isin(sel_ufs)
        perfil_filt = df_perfil[mask_p]

        if not perfil_filt.empty:
            col_a, col_b = st.columns(2)

            with col_a:
                st.markdown("**Por Sexo**")
                por_sexo = perfil_filt.groupby("sexo", as_index=False).agg(
                    saldo=("saldo", "sum"),
                    salario_medio=("salario_medio", "mean"),
                )
                fig = px.bar(
                    por_sexo, x="sexo", y="saldo",
                    labels={"saldo": "Saldo", "sexo": "Sexo"},
                    text_auto=",.0f",
                    color_discrete_sequence=["#3498db"],
                )
                fig.update_layout(height=300, margin=dict(t=10))
                st.plotly_chart(fig, use_container_width=True)

            with col_b:
                st.markdown("**Por Grau de Instrucao**")
                por_instrucao = perfil_filt.groupby("grau_instrucao", as_index=False).agg(
                    saldo=("saldo", "sum"),
                )
                por_instrucao.sort_values("saldo", ascending=True, inplace=True)
                fig = px.bar(
                    por_instrucao, x="saldo", y="grau_instrucao", orientation="h",
                    labels={"saldo": "Saldo", "grau_instrucao": "Escolaridade"},
                    text_auto=",.0f",
                    color_discrete_sequence=["#8e44ad"],
                )
                fig.update_layout(
                    height=max(300, len(por_instrucao) * 30), margin=dict(t=10, l=200),
                    yaxis_type="category",
                )
                st.plotly_chart(fig, use_container_width=True)

# ============================================================================
# ABA RAIS
# ============================================================================

with tab_rais:
    if not tem_rais:
        st.info("Dados da RAIS nao encontrados. Execute a coleta.")
        st.stop()

    df_rais = pd.read_csv(rais_vinculos_path)
    if "uf_nome" not in df_rais.columns:
        df_rais["uf_nome"] = df_rais["sigla_uf"].map(UF_NOMES)

    # --- Filtros ---
    col1, col2 = st.columns(2)
    with col1:
        anos_r = sorted(df_rais["ano"].unique())
        sel_anos_r = st.slider(
            "Periodo",
            int(min(anos_r)), int(max(anos_r)),
            (int(min(anos_r)), int(max(anos_r))),
            key="rais_ano",
        )
    with col2:
        ufs_r = sorted(df_rais["sigla_uf"].unique())
        sel_ufs_r = st.multiselect(
            "Estados", ufs_r, default=ufs_r, key="rais_uf",
        )

    mask_r = df_rais["ano"].between(*sel_anos_r) & df_rais["sigla_uf"].isin(sel_ufs_r)
    filt_r = df_rais[mask_r]

    # --- Metricas ---
    if not filt_r.empty:
        ultimo_ano = filt_r[filt_r["ano"] == filt_r["ano"].max()]
        c1, c2, c3 = st.columns(3)
        c1.metric(
            f"Vinculos Ativos ({int(filt_r['ano'].max())})",
            f"{ultimo_ano['vinculos_ativos'].sum():,.0f}",
        )
        c2.metric(
            "Remuneracao Media",
            f"R$ {ultimo_ano['remuneracao_media'].mean():,.2f}",
        )
        if "horas_media" in ultimo_ano.columns:
            c3.metric(
                "Horas Contratadas (media)",
                f"{ultimo_ano['horas_media'].mean():,.1f}h",
            )

    st.divider()

    # --- Evolucao do estoque de vinculos ---
    st.subheader("Evolucao do Estoque de Vinculos Formais")
    vinculos_ano = filt_r.groupby("ano", as_index=False)["vinculos_ativos"].sum()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=vinculos_ano["ano"], y=vinculos_ano["vinculos_ativos"],
        marker_color="#3498db", name="Vinculos",
    ))
    fig.update_layout(
        hovermode="x unified", height=400, margin=dict(t=10),
        yaxis_title="Vinculos Ativos", yaxis_tickformat=",d",
        xaxis_dtick=1,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Comparacao entre estados ---
    st.subheader(f"Vinculos por Estado — {int(filt_r['ano'].max())}")
    if not ultimo_ano.empty:
        uf_rank = ultimo_ano[["sigla_uf", "vinculos_ativos", "remuneracao_media"]].copy()
        uf_rank["uf_nome"] = uf_rank["sigla_uf"].map(UF_NOMES)
        uf_rank.sort_values("vinculos_ativos", ascending=True, inplace=True)

        fig = px.bar(
            uf_rank, x="vinculos_ativos", y="uf_nome", orientation="h",
            labels={"vinculos_ativos": "Vinculos Ativos", "uf_nome": ""},
            text_auto=",.0f",
            color_discrete_sequence=["#2ecc71"],
        )
        fig.update_layout(
            height=400, margin=dict(t=10, l=200),
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- Evolucao por UF ---
    st.subheader("Evolucao por Estado")
    fig = px.line(
        filt_r, x="ano", y="vinculos_ativos", color="uf_nome",
        labels={"vinculos_ativos": "Vinculos", "ano": "Ano", "uf_nome": "Estado"},
        markers=True,
    )
    fig.update_layout(
        hovermode="x unified", height=400, margin=dict(t=10),
        yaxis_tickformat=",d", xaxis_dtick=1,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Remuneracao media por UF ---
    st.subheader("Remuneracao Media por Estado")
    fig = px.line(
        filt_r, x="ano", y="remuneracao_media", color="uf_nome",
        labels={"remuneracao_media": "R$ Remuneracao Media", "ano": "Ano", "uf_nome": "Estado"},
        markers=True,
    )
    fig.update_layout(
        hovermode="x unified", height=400, margin=dict(t=10),
        yaxis_tickprefix="R$ ", yaxis_tickformat=",.2f",
        xaxis_dtick=1,
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Analise setorial RAIS ---
    if rais_setor_path.exists():
        st.subheader("Vinculos por Setor Economico (Divisao CNAE)")
        df_rais_setor = pd.read_csv(rais_setor_path)
        mask_rs = df_rais_setor["ano"].between(*sel_anos_r) & df_rais_setor["sigla_uf"].isin(sel_ufs_r)
        rais_setor_filt = df_rais_setor[mask_rs]

        if not rais_setor_filt.empty:
            ultimo_setor = rais_setor_filt[rais_setor_filt["ano"] == rais_setor_filt["ano"].max()]
            setor_agg = ultimo_setor.groupby("divisao_cnae", as_index=False).agg(
                vinculos=("vinculos_ativos", "sum"),
                remuneracao_media=("remuneracao_media", "mean"),
            )
            setor_agg.sort_values("vinculos", ascending=True, inplace=True)
            top_setores = setor_agg.tail(15)

            fig = px.bar(
                top_setores, x="vinculos", y="divisao_cnae", orientation="h",
                labels={"vinculos": "Vinculos", "divisao_cnae": "Divisao CNAE"},
                text_auto=",.0f",
                color_discrete_sequence=["#e67e22"],
            )
            fig.update_layout(
                height=max(400, len(top_setores) * 30), margin=dict(t=10, l=150),
                yaxis_type="category",
            )
            st.plotly_chart(fig, use_container_width=True)

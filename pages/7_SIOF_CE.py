import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="SIOF-CE", layout="wide")
st.title("SIOF — Execução Orçamentária do Ceará")

PROC = PROCESSED_DIR

# ==============================================================================
# Carregamento de dados
# ==============================================================================

# Dados gerais por secretaria (parquet legado ou CSV novo)
path_parquet = PROC / "siof_ce.parquet"
path_csv = PROC / "execucao_orcamentaria" / "ce" / "siof_ce.csv"

if path_parquet.exists():
    df = pd.read_parquet(path_parquet)
elif path_csv.exists():
    df = pd.read_csv(path_csv)
else:
    df = pd.DataFrame()

# Dados de obras (elem 51)
path_obras_sec = PROC / "execucao_orcamentaria" / "ce" / "siof_obras_secretaria.csv"
path_obras_reg = PROC / "execucao_orcamentaria" / "ce" / "siof_obras_regiao.csv"

df_obras = pd.read_csv(path_obras_sec) if path_obras_sec.exists() else pd.DataFrame()
df_obras_reg = pd.read_csv(path_obras_reg) if path_obras_reg.exists() else pd.DataFrame()

tem_geral = not df.empty
tem_obras = not df_obras.empty
tem_regioes = not df_obras_reg.empty

if not tem_geral and not tem_obras:
    st.warning("Dados do SIOF-CE não encontrados. Execute a coleta e o ETL.")
    st.stop()

# Limpar nulos
if tem_geral:
    df = df.dropna(subset=["Descrição"])
if tem_obras:
    df_obras = df_obras.dropna(subset=["Descrição"])

# ==============================================================================
# Abas: Execução Geral | Obras e Instalações
# ==============================================================================

tabs = []
tab_names = []
if tem_geral:
    tab_names.append("Execução Geral")
if tem_obras:
    tab_names.append("Obras e Instalações")

if len(tab_names) == 0:
    st.stop()

tabs = st.tabs(tab_names)
tab_idx = 0

# ==============================================================================
# ABA 1 — Execução Geral (visualização original)
# ==============================================================================

if tem_geral:
    with tabs[tab_idx]:
        # --- Filtros ---
        col1, col2 = st.columns(2)
        with col1:
            anos = sorted(df["ano"].unique())
            sel_anos = st.slider(
                "Período", int(min(anos)), int(max(anos)),
                (int(min(anos)), int(max(anos))), key="siof_ano",
            )
        with col2:
            secretarias = sorted(df["Descrição"].unique())
            ultimo_ano = df[df["ano"] == max(anos)]
            top10 = ultimo_ano.nlargest(10, "Empenhado")["Descrição"].tolist()
            sel_sec = st.multiselect(
                "Secretarias", secretarias, default=top10, key="siof_sec",
            )

        mask = df["ano"].between(*sel_anos) & df["Descrição"].isin(sel_sec)
        filt = df[mask]

        # --- Métricas do último ano selecionado ---
        ult = filt[filt["ano"] == sel_anos[1]]
        if not ult.empty:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Dotação (Lei + Créd.)", f"R$ {ult['Lei + Cred.'].sum() / 1e9:.1f} bi")
            c2.metric("Empenhado", f"R$ {ult['Empenhado'].sum() / 1e9:.1f} bi")
            c3.metric("Pago", f"R$ {ult['Pago'].sum() / 1e9:.1f} bi")
            exec_pct = ult["Pago"].sum() / ult["Lei + Cred."].sum() * 100 if ult["Lei + Cred."].sum() > 0 else 0
            c4.metric("Execução (Pago/Dotação)", f"{exec_pct:.1f}%")

        st.divider()

        # --- Evolução anual agregada ---
        st.subheader("Evolução Anual — Dotação, Empenhado e Pago")
        evol = filt.groupby("ano", as_index=False).agg(
            dotacao=("Lei + Cred.", "sum"),
            empenhado=("Empenhado", "sum"),
            pago=("Pago", "sum"),
        )
        fig = go.Figure()
        fig.add_trace(go.Bar(x=evol["ano"], y=evol["dotacao"], name="Dotação (Lei+Créd.)", marker_color="#bdc3c7"))
        fig.add_trace(go.Bar(x=evol["ano"], y=evol["empenhado"], name="Empenhado", marker_color="#3498db"))
        fig.add_trace(go.Bar(x=evol["ano"], y=evol["pago"], name="Pago", marker_color="#2ecc71"))
        fig.update_layout(
            barmode="group", hovermode="x unified", height=400, margin=dict(t=10),
            yaxis_title="R$", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
            xaxis_dtick=1,
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- Ranking por secretaria (último ano selecionado) ---
        st.subheader(f"Ranking por Secretaria — {sel_anos[1]}")
        if not ult.empty:
            rank = ult[["Descrição", "Empenhado", "Pago"]].copy()
            rank.sort_values("Empenhado", ascending=True, inplace=True)
            fig = px.bar(
                rank, x="Empenhado", y="Descrição", orientation="h",
                labels={"Empenhado": "R$ Empenhado", "Descrição": ""},
                text_auto=".3s",
            )
            fig.update_layout(
                height=max(400, len(rank) * 25), margin=dict(t=10, l=300),
                xaxis_tickprefix="R$ ", xaxis_tickformat=",.0f",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados para o ano selecionado.")

        # --- Taxa de execução por secretaria ---
        st.subheader(f"Taxa de Execução (% Pago) — {sel_anos[1]}")
        if not ult.empty:
            exec_df = ult[["Descrição", "% Pago"]].copy()
            exec_df.sort_values("% Pago", ascending=True, inplace=True)
            fig = px.bar(
                exec_df, x="% Pago", y="Descrição", orientation="h",
                labels={"% Pago": "% Pago", "Descrição": ""},
                text_auto=".1f",
                color="% Pago",
                color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                range_color=[50, 100],
            )
            fig.update_layout(
                height=max(400, len(exec_df) * 25), margin=dict(t=10, l=300),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Evolução por secretaria selecionada ---
        st.subheader("Evolução por Secretaria")
        sel_detalhe = st.selectbox(
            "Secretaria", sorted(filt["Descrição"].unique()), key="siof_det",
        )
        det = filt[filt["Descrição"] == sel_detalhe]
        if not det.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=det["ano"], y=det["Lei + Cred."], name="Dotação", line=dict(dash="dot", color="#bdc3c7"),
            ))
            fig.add_trace(go.Scatter(
                x=det["ano"], y=det["Empenhado"], name="Empenhado", line=dict(width=2, color="#3498db"),
            ))
            fig.add_trace(go.Scatter(
                x=det["ano"], y=det["Pago"], name="Pago", line=dict(width=2, color="#2ecc71"),
            ))
            fig.update_layout(
                hovermode="x unified", height=400, margin=dict(t=10),
                yaxis_title="R$", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
                xaxis_dtick=1,
            )
            st.plotly_chart(fig, use_container_width=True)

    tab_idx += 1

# ==============================================================================
# ABA 2 — Obras e Instalações (Elemento 51)
# ==============================================================================

if tem_obras:
    with tabs[tab_idx]:
        st.markdown("**Elemento de Despesa 51** — Obras e Instalações (Investimentos)")

        # --- Filtros ---
        anos_obras = sorted(df_obras["ano"].unique())
        col1, col2 = st.columns(2)
        with col1:
            sel_anos_ob = st.slider(
                "Período", int(min(anos_obras)), int(max(anos_obras)),
                (int(min(anos_obras)), int(max(anos_obras))), key="obras_ano",
            )
        with col2:
            secs_obras = sorted(df_obras["Descrição"].unique())
            ult_ano_obras = df_obras[df_obras["ano"] == max(anos_obras)]
            top_obras = ult_ano_obras.nlargest(10, "Empenhado")["Descrição"].tolist()
            sel_sec_ob = st.multiselect(
                "Secretarias", secs_obras, default=top_obras, key="obras_sec",
            )

        mask_ob = df_obras["ano"].between(*sel_anos_ob) & df_obras["Descrição"].isin(sel_sec_ob)
        filt_ob = df_obras[mask_ob]

        # --- Métricas ---
        ult_ob = filt_ob[filt_ob["ano"] == sel_anos_ob[1]]
        if not ult_ob.empty:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Dotação Obras", f"R$ {ult_ob['Lei + Cred.'].sum() / 1e9:.2f} bi")
            c2.metric("Empenhado", f"R$ {ult_ob['Empenhado'].sum() / 1e9:.2f} bi")
            c3.metric("Pago", f"R$ {ult_ob['Pago'].sum() / 1e9:.2f} bi")
            exec_ob = ult_ob["Pago"].sum() / ult_ob["Lei + Cred."].sum() * 100 if ult_ob["Lei + Cred."].sum() > 0 else 0
            c4.metric("Execução (Pago/Dotação)", f"{exec_ob:.1f}%")

        st.divider()

        # --- Evolução anual de obras ---
        st.subheader("Evolução Anual — Obras e Instalações")
        evol_ob = filt_ob.groupby("ano", as_index=False).agg(
            dotacao=("Lei + Cred.", "sum"),
            empenhado=("Empenhado", "sum"),
            pago=("Pago", "sum"),
        )
        fig = go.Figure()
        fig.add_trace(go.Bar(x=evol_ob["ano"], y=evol_ob["dotacao"], name="Dotação", marker_color="#bdc3c7"))
        fig.add_trace(go.Bar(x=evol_ob["ano"], y=evol_ob["empenhado"], name="Empenhado", marker_color="#e67e22"))
        fig.add_trace(go.Bar(x=evol_ob["ano"], y=evol_ob["pago"], name="Pago", marker_color="#27ae60"))
        fig.update_layout(
            barmode="group", hovermode="x unified", height=400, margin=dict(t=10),
            yaxis_title="R$", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
            xaxis_dtick=1,
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- Ranking por secretaria ---
        st.subheader(f"Ranking de Obras por Secretaria — {sel_anos_ob[1]}")
        if not ult_ob.empty:
            rank_ob = ult_ob[ult_ob["Empenhado"] > 0][["Descrição", "Empenhado", "Pago"]].copy()
            rank_ob.sort_values("Empenhado", ascending=True, inplace=True)
            fig = px.bar(
                rank_ob, x="Empenhado", y="Descrição", orientation="h",
                labels={"Empenhado": "R$ Empenhado", "Descrição": ""},
                text_auto=".3s",
                color_discrete_sequence=["#e67e22"],
            )
            fig.update_layout(
                height=max(400, len(rank_ob) * 28), margin=dict(t=10, l=300),
                xaxis_tickprefix="R$ ", xaxis_tickformat=",.0f",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados para o ano selecionado.")

        # --- Taxa de execução de obras ---
        st.subheader(f"Taxa de Execução de Obras (% Pago) — {sel_anos_ob[1]}")
        if not ult_ob.empty:
            exec_ob_df = ult_ob[ult_ob["Lei + Cred."] > 0][["Descrição", "Pago", "Lei + Cred."]].copy()
            exec_ob_df["% Pago"] = (exec_ob_df["Pago"] / exec_ob_df["Lei + Cred."]) * 100
            exec_ob_df.sort_values("% Pago", ascending=True, inplace=True)
            fig = px.bar(
                exec_ob_df, x="% Pago", y="Descrição", orientation="h",
                labels={"% Pago": "% Pago", "Descrição": ""},
                text_auto=".1f",
                color="% Pago",
                color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                range_color=[0, 100],
            )
            fig.update_layout(
                height=max(400, len(exec_ob_df) * 28), margin=dict(t=10, l=300),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Evolução por secretaria ---
        st.subheader("Evolução de Obras por Secretaria")
        sel_det_ob = st.selectbox(
            "Secretaria", sorted(filt_ob["Descrição"].unique()), key="obras_det",
        )
        det_ob = filt_ob[filt_ob["Descrição"] == sel_det_ob]
        if not det_ob.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=det_ob["ano"], y=det_ob["Lei + Cred."], name="Dotação",
                line=dict(dash="dot", color="#bdc3c7"),
            ))
            fig.add_trace(go.Scatter(
                x=det_ob["ano"], y=det_ob["Empenhado"], name="Empenhado",
                line=dict(width=2, color="#e67e22"),
            ))
            fig.add_trace(go.Scatter(
                x=det_ob["ano"], y=det_ob["Pago"], name="Pago",
                line=dict(width=2, color="#27ae60"),
            ))
            fig.update_layout(
                hovermode="x unified", height=400, margin=dict(t=10),
                yaxis_title="R$", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
                xaxis_dtick=1,
            )
            st.plotly_chart(fig, use_container_width=True)

        # --- Obras por Região (quando disponível) ---
        if tem_regioes:
            st.divider()
            st.subheader("Obras por Região do Ceará")
            st.caption("Dados detalhados por região disponíveis para o ano corrente (relatório hierárquico do SIOF)")

            # Excluir "Estado do Ceará" (não é uma região geográfica)
            df_reg_plot = df_obras_reg[df_obras_reg["regiao"] != "Estado do Ceará"].copy()

            if not df_reg_plot.empty:
                df_reg_plot.sort_values("empenhado", ascending=True, inplace=True)

                c1, c2 = st.columns(2)

                with c1:
                    st.markdown("**Empenhado por Região**")
                    fig = px.bar(
                        df_reg_plot, x="empenhado", y="regiao", orientation="h",
                        labels={"empenhado": "R$ Empenhado", "regiao": ""},
                        text_auto=".3s",
                        color="empenhado",
                        color_continuous_scale=["#fadbd8", "#e74c3c"],
                    )
                    fig.update_layout(
                        height=max(400, len(df_reg_plot) * 30),
                        margin=dict(t=10, l=200),
                        xaxis_tickprefix="R$ ", xaxis_tickformat=",.0f",
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with c2:
                    st.markdown("**Quantidade de Ações por Região**")
                    df_acoes = df_reg_plot.sort_values("n_acoes", ascending=True)
                    fig = px.bar(
                        df_acoes, x="n_acoes", y="regiao", orientation="h",
                        labels={"n_acoes": "Ações orçamentárias", "regiao": ""},
                        text_auto="d",
                        color_discrete_sequence=["#3498db"],
                    )
                    fig.update_layout(
                        height=max(400, len(df_acoes) * 30),
                        margin=dict(t=10, l=200),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Tabela resumo
                st.markdown("**Resumo por Região**")
                tabela = df_reg_plot.sort_values("empenhado", ascending=False).copy()
                tabela["dotacao"] = tabela["dotacao"].apply(lambda x: f"R$ {x/1e6:,.1f}M")
                tabela["empenhado"] = tabela["empenhado"].apply(lambda x: f"R$ {x/1e6:,.1f}M")
                tabela["pago"] = tabela["pago"].apply(lambda x: f"R$ {x/1e6:,.1f}M")
                tabela = tabela.rename(columns={
                    "regiao": "Região", "dotacao": "Dotação",
                    "empenhado": "Empenhado", "pago": "Pago",
                    "n_acoes": "Ações", "ano": "Ano",
                })
                st.dataframe(tabela[["Região", "Dotação", "Empenhado", "Pago", "Ações"]], hide_index=True, use_container_width=True)

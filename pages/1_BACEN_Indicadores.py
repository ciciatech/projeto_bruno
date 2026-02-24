import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(page_title="BACEN Indicadores", layout="wide")
st.title("BACEN — Indicadores Econômicos")

PROC = Path("dados_nordeste/processed")
df = pd.read_parquet(PROC / "bacen.parquet")

min_date, max_date = df["data"].min().date(), df["data"].max().date()
d_inicio, d_fim = st.slider(
    "Período",
    min_value=min_date, max_value=max_date,
    value=(min_date, max_date),
    format="MM/YYYY",
)
df = df[(df["data"].dt.date >= d_inicio) & (df["data"].dt.date <= d_fim)]

# --- IBCR ---
st.subheader("Índice de Atividade Econômica — IBCR Nordeste")
fig = px.line(df, x="data", y="IBCR_NE_ajuste_sazonal", labels={"data": "", "IBCR_NE_ajuste_sazonal": "IBCR-NE"})
fig.update_layout(hovermode="x unified", height=350, margin=dict(t=10))
st.plotly_chart(fig, use_container_width=True)

# --- SELIC e IPCA ---
st.subheader("SELIC e IPCA")
col1, col2 = st.columns(2)
with col1:
    fig_selic = px.line(df, x="data", y="selic_mensal", labels={"data": "", "selic_mensal": "SELIC (% a.a.)"})
    fig_selic.update_layout(height=300, margin=dict(t=10))
    st.plotly_chart(fig_selic, use_container_width=True)
with col2:
    fig_ipca = px.line(df, x="data", y="ipca_mensal", labels={"data": "", "ipca_mensal": "IPCA (% mensal)"})
    fig_ipca.update_traces(line_color="#e74c3c")
    fig_ipca.update_layout(height=300, margin=dict(t=10))
    st.plotly_chart(fig_ipca, use_container_width=True)

# --- Crédito NE vs Brasil ---
st.subheader("Saldo de Crédito — Nordeste vs Brasil")
tab_pf, tab_pj = st.tabs(["Pessoa Física", "Pessoa Jurídica"])

with tab_pf:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["data"], y=df["credito_PF_nordeste"], name="PF Nordeste", line=dict(width=2)))
    fig.add_trace(go.Scatter(x=df["data"], y=df["credito_PF_brasil"], name="PF Brasil", line=dict(dash="dot")))
    fig.update_layout(hovermode="x unified", height=350, margin=dict(t=10), yaxis_title="R$ milhões")
    st.plotly_chart(fig, use_container_width=True)

with tab_pj:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["data"], y=df["credito_PJ_nordeste"], name="PJ Nordeste", line=dict(width=2)))
    fig.add_trace(go.Scatter(x=df["data"], y=df["credito_PJ_brasil"], name="PJ Brasil", line=dict(dash="dot")))
    fig.update_layout(hovermode="x unified", height=350, margin=dict(t=10), yaxis_title="R$ milhões")
    st.plotly_chart(fig, use_container_width=True)

# --- Inadimplência ---
st.subheader("Taxa de Inadimplência")
fig = go.Figure()
fig.add_trace(go.Scatter(x=df["data"], y=df["inadimplencia_PF"], name="PF"))
fig.add_trace(go.Scatter(x=df["data"], y=df["inadimplencia_PJ"], name="PJ"))
fig.update_layout(hovermode="x unified", height=350, margin=dict(t=10), yaxis_title="%")
st.plotly_chart(fig, use_container_width=True)

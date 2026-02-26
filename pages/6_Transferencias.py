import streamlit as st
import pandas as pd
import plotly.express as px

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="Transferências", layout="wide")
st.title("Transferências Constitucionais")

PROC = PROCESSED_DIR
df = pd.read_parquet(PROC / "transferencias.parquet")

# --- Filtros ---
col1, col2, col3 = st.columns(3)
with col1:
    ufs = sorted(df["uf"].unique())
    sel_ufs = st.multiselect("Estados", ufs, default=ufs, key="tr_uf")
with col2:
    anos = sorted(df["exercicio"].unique())
    sel_anos = st.slider("Período", int(min(anos)), int(max(anos)), (int(min(anos)), int(max(anos))), key="tr_ano")
with col3:
    contas = sorted(df["conta"].unique())
    top_contas = [c for c in contas if any(t in c.lower() for t in ["correntes", "capital", "fpe", "constitucionais"])]
    sel_contas = st.multiselect("Tipo de transferência", contas, default=top_contas or contas[:5], key="tr_conta")

mask = df["uf"].isin(sel_ufs) & df["exercicio"].between(*sel_anos) & df["conta"].isin(sel_contas)
filt = df[mask]

# --- Acumulado anual (último bimestre) ---
anual = filt.loc[filt.groupby(["exercicio", "uf", "cod_conta"])["periodo"].idxmax()]

# --- Ranking por UF ---
st.subheader("Total Transferido por Estado")
rank = anual.groupby("uf_nome", as_index=False)["valor"].sum().sort_values("valor", ascending=True)
if not rank.empty:
    fig = px.bar(
        rank, x="valor", y="uf_nome", orientation="h",
        labels={"valor": "R$ (acumulado)", "uf_nome": ""},
        text_auto=".3s",
    )
    fig.update_layout(height=400, margin=dict(t=10), xaxis_tickprefix="R$ ")
    st.plotly_chart(fig, use_container_width=True)

# --- Evolução anual ---
st.subheader("Evolução Anual por Estado")
evol = anual.groupby(["exercicio", "uf_nome"], as_index=False)["valor"].sum()
fig = px.line(
    evol, x="exercicio", y="valor", color="uf_nome",
    labels={"exercicio": "Ano", "valor": "R$", "uf_nome": "Estado"},
    markers=True,
)
fig.update_layout(hovermode="x unified", height=400, margin=dict(t=10))
fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
st.plotly_chart(fig, use_container_width=True)

# --- Breakdown por tipo ---
st.subheader("Detalhamento por Tipo de Transferência")
sel_uf_det = st.selectbox("Estado", sorted(filt["uf_nome"].dropna().unique()), key="tr_det")
det = anual[anual["uf_nome"] == sel_uf_det]
if not det.empty:
    det_agg = det.groupby(["exercicio", "conta"], as_index=False)["valor"].sum()
    fig = px.bar(
        det_agg, x="exercicio", y="valor", color="conta",
        labels={"exercicio": "Ano", "valor": "R$", "conta": ""},
    )
    fig.update_layout(height=450, margin=dict(t=10))
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)

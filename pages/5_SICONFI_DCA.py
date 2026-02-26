import streamlit as st
import pandas as pd
import plotly.express as px

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="SICONFI DCA", layout="wide")
st.title("SICONFI — Balanço Patrimonial (DCA)")

PROC = PROCESSED_DIR
df = pd.read_parquet(PROC / "dca_resumo.parquet")

# --- Filtros ---
col1, col2 = st.columns(2)
with col1:
    ufs = sorted(df["uf"].unique())
    sel_ufs = st.multiselect("Estados", ufs, default=ufs, key="dca_uf")
with col2:
    anos = sorted(df["exercicio"].unique())
    sel_anos = st.slider("Período", int(min(anos)), int(max(anos)), (int(min(anos)), int(max(anos))), key="dca_ano")

mask = df["uf"].isin(sel_ufs) & df["exercicio"].between(*sel_anos)
filt = df[mask]

# --- Ativo vs Passivo ---
st.subheader("Ativo Total vs Passivo Total por Estado")
ap = filt[filt["conta_resumo"].isin(["Ativo Total", "Passivo Total"])]
fig = px.bar(
    ap, x="uf_nome", y="valor", color="conta_resumo", facet_col="exercicio", facet_col_wrap=4,
    labels={"uf_nome": "", "valor": "R$", "conta_resumo": ""},
    barmode="group",
)
fig.update_layout(height=550, margin=dict(t=30))
fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
st.plotly_chart(fig, use_container_width=True)

# --- Patrimônio Líquido ---
st.subheader("Patrimônio Líquido — Evolução Anual")
pl = filt[filt["conta_resumo"] == "Patrimônio Líquido"]
if not pl.empty:
    fig = px.line(
        pl, x="exercicio", y="valor", color="uf_nome",
        labels={"exercicio": "Ano", "valor": "R$", "uf_nome": "Estado"},
        markers=True,
    )
    fig.update_layout(hovermode="x unified", height=400, margin=dict(t=10))
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Dados de patrimônio líquido não disponíveis.")

# --- Composição ---
st.subheader("Composição Patrimonial")
comp = filt[filt["conta_resumo"].isin(["Ativo Circulante", "Ativo Não Circulante", "Passivo Circulante", "Passivo Não Circulante", "Patrimônio Líquido"])]
sel_uf_comp = st.selectbox("Estado", sorted(comp["uf_nome"].dropna().unique()), key="dca_comp")
comp_uf = comp[comp["uf_nome"] == sel_uf_comp]

if not comp_uf.empty:
    fig = px.bar(
        comp_uf, x="exercicio", y="valor", color="conta_resumo",
        labels={"exercicio": "Ano", "valor": "R$", "conta_resumo": ""},
        barmode="relative",
    )
    fig.update_layout(height=400, margin=dict(t=10))
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)

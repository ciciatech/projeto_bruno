import streamlit as st
import pandas as pd
import plotly.express as px

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="SICONFI RREO", layout="wide")
st.title("SICONFI — Execução Orçamentária (RREO)")

PROC = PROCESSED_DIR
df = pd.read_parquet(PROC / "rreo_resumo.parquet")

# --- Filtros ---
col1, col2, col3 = st.columns(3)
with col1:
    ufs = sorted(df["uf"].unique())
    sel_ufs = st.multiselect("Estados", ufs, default=ufs, key="rreo_uf")
with col2:
    anos = sorted(df["exercicio"].unique())
    sel_anos = st.slider("Período", int(min(anos)), int(max(anos)), (int(min(anos)), int(max(anos))))
with col3:
    contas_disp = sorted(df["conta"].unique())
    sel_contas = st.multiselect("Contas", contas_disp, default=[
        "RECEITAS (EXCETO INTRA-ORÇAMENTÁRIAS) (I)",
        "RECEITAS CORRENTES",
        "TRANSFERÊNCIAS CORRENTES",
    ])

mask = (
    df["uf"].isin(sel_ufs)
    & df["exercicio"].between(*sel_anos)
    & df["conta"].isin(sel_contas)
)
filt = df[mask]

# --- Último bimestre de cada ano (acumulado anual) ---
anual = filt.loc[filt.groupby(["exercicio", "uf", "cod_conta"])["periodo"].idxmax()]

# --- Evolução por ano ---
st.subheader("Evolução Anual por Estado")
fig = px.line(
    anual, x="exercicio", y="valor", color="uf_nome", facet_col="conta", facet_col_wrap=2,
    labels={"exercicio": "Ano", "valor": "R$", "uf_nome": "Estado"},
)
fig.update_layout(height=600, margin=dict(t=30))
fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
st.plotly_chart(fig, use_container_width=True)

# --- Comparativo por UF (último ano) ---
st.subheader(f"Comparativo por Estado — {sel_anos[1]}")
ult = anual[anual["exercicio"] == sel_anos[1]]
if not ult.empty:
    fig = px.bar(
        ult, x="uf_nome", y="valor", color="conta", barmode="group",
        labels={"uf_nome": "", "valor": "R$", "conta": "Conta"},
        text_auto=".3s",
    )
    fig.update_layout(height=450, margin=dict(t=10))
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados para o ano selecionado.")

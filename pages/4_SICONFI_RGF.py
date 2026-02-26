import streamlit as st
import pandas as pd
import plotly.express as px

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="SICONFI RGF", layout="wide")
st.title("SICONFI — Gestão Fiscal (RGF)")

PROC = PROCESSED_DIR
df = pd.read_parquet(PROC / "rgf_resumo.parquet")

# --- Filtros ---
col1, col2 = st.columns(2)
with col1:
    ufs = sorted(df["uf"].unique())
    sel_ufs = st.multiselect("Estados", ufs, default=ufs, key="rgf_uf")
with col2:
    anos = sorted(df["exercicio"].unique())
    sel_anos = st.slider("Período", int(min(anos)), int(max(anos)), (int(min(anos)), int(max(anos))), key="rgf_ano")

mask = df["uf"].isin(sel_ufs) & df["exercicio"].between(*sel_anos)
filt = df[mask]

# --- Despesa com Pessoal ---
st.subheader("Despesa com Pessoal")
pessoal = filt[filt["anexo"] == "RGF-Anexo 01"]
contas_pessoal = ["DespesaComPessoalBruta", "DespesaComPessoalLiquida", "ReceitaCorrenteLiquidaLimiteLegal"]
pessoal = pessoal[pessoal["cod_conta"].isin(contas_pessoal)]

ult_periodo = pessoal.groupby(["exercicio", "uf", "cod_conta"])["periodo"].max().reset_index()
pessoal = pessoal.merge(ult_periodo, on=["exercicio", "uf", "cod_conta", "periodo"])

fig = px.line(
    pessoal, x="exercicio", y="valor", color="uf_nome", facet_col="conta", facet_col_wrap=2,
    labels={"exercicio": "Ano", "valor": "R$", "uf_nome": "Estado"},
)
fig.update_layout(height=500, margin=dict(t=30))
fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
st.plotly_chart(fig, use_container_width=True)

# --- Dívida Consolidada ---
st.subheader("Dívida Consolidada")
divida = filt[filt["anexo"] == "RGF-Anexo 02"]
contas_divida = ["DividaConsolidada", "DividaConsolidadaLiquida"]
divida = divida[divida["cod_conta"].isin(contas_divida)]

ult_div = divida.groupby(["exercicio", "uf", "cod_conta"])["periodo"].max().reset_index()
divida = divida.merge(ult_div, on=["exercicio", "uf", "cod_conta", "periodo"])

if not divida.empty:
    fig = px.bar(
        divida, x="uf_nome", y="valor", color="conta", facet_col="exercicio", facet_col_wrap=4,
        labels={"uf_nome": "", "valor": "R$", "conta": ""},
        barmode="group",
    )
    fig.update_layout(height=500, margin=dict(t=30), showlegend=True)
    fig.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Dados de dívida consolidada não disponíveis para a seleção.")

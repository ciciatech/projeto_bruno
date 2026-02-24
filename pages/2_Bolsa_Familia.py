import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Bolsa Família", layout="wide")
st.title("Novo Bolsa Família — Capitais do Nordeste")

PROC = Path("dados_nordeste/processed")
path = PROC / "bolsa_familia.parquet"
if not path.exists():
    st.warning("Dados do Bolsa Família não encontrados. Execute o ETL.")
    st.stop()

df = pd.read_parquet(path)

capitais = sorted(df["municipio"].unique())
sel = st.multiselect("Capitais", capitais, default=capitais)
df = df[df["municipio"].isin(sel)]

# --- Métricas ---
c1, c2, c3 = st.columns(3)
c1.metric("Valor total", f"R$ {df['valor'].sum() / 1e9:.1f} bi")
c2.metric("Beneficiados (soma mensal)", f"{df['qtd_beneficiados'].sum() / 1e6:.1f} mi")
c3.metric("Meses cobertos", f"{df['data'].nunique()}")

st.divider()

# --- Evolução mensal ---
st.subheader("Evolução Mensal do Valor Pago")
fig = px.line(
    df, x="data", y="valor", color="municipio",
    labels={"data": "", "valor": "R$", "municipio": "Capital"},
)
fig.update_layout(hovermode="x unified", height=400, margin=dict(t=10), yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f")
st.plotly_chart(fig, use_container_width=True)

# --- Ranking ---
st.subheader("Ranking — Valor Total por Capital")
rank = df.groupby("municipio", as_index=False).agg(
    valor_total=("valor", "sum"),
    beneficiados_media=("qtd_beneficiados", "mean"),
)
rank.sort_values("valor_total", ascending=True, inplace=True)

fig = px.bar(
    rank, x="valor_total", y="municipio", orientation="h",
    labels={"valor_total": "Valor total (R$)", "municipio": ""},
    text_auto=".3s",
)
fig.update_layout(height=400, margin=dict(t=10), xaxis_tickprefix="R$ ")
st.plotly_chart(fig, use_container_width=True)

# --- Beneficiados ---
st.subheader("Beneficiados — Evolução Mensal")
fig = px.area(
    df, x="data", y="qtd_beneficiados", color="municipio",
    labels={"data": "", "qtd_beneficiados": "Beneficiados", "municipio": "Capital"},
)
fig.update_layout(hovermode="x unified", height=400, margin=dict(t=10))
st.plotly_chart(fig, use_container_width=True)

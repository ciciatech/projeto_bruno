"""Página: Painel Regional CE — 14 regiões SEPLAG/IPECE × meses."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="Painel Regional CE", layout="wide")
st.title("Painel Regional Ceará — 14 regiões SEPLAG/IPECE")
st.caption(
    "Pipeline regional CE (resposta ao pedido do Prof. Paulo, abr/2026). "
    "Cobertura crescerá conforme as coletas no Mac Mini avançam."
)

PAINEL = PROCESSED_DIR / "model_ready" / "painel_regional_ce_mensal.csv"

if not PAINEL.exists():
    st.error(f"Painel não encontrado: `{PAINEL}`")
    st.info(
        "Rode `python -m pipeline.run --painel-ce` para gerar, ou aguarde "
        "as coletas terminarem no Mac Mini."
    )
    st.stop()

df = pd.read_csv(PAINEL, dtype={"regiao_codigo": str})
df["data"] = pd.to_datetime(
    df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01"
)

# ---- Sumário no topo ----
total_linhas = len(df)
total_regioes = df["regiao_codigo"].nunique()
total_meses = df.groupby(["ano", "mes"]).ngroups
periodo_min = f"{df['ano'].min():.0f}/{df[df['ano']==df['ano'].min()]['mes'].min():02.0f}"
periodo_max = f"{df['ano'].max():.0f}/{df[df['ano']==df['ano'].max()]['mes'].max():02.0f}"

c1, c2, c3, c4 = st.columns(4)
c1.metric("Linhas", f"{total_linhas:,}")
c2.metric("Regiões", total_regioes)
c3.metric("Meses", total_meses)
c4.metric("Período", f"{periodo_min} a {periodo_max}")

# ---- Cobertura ----
st.subheader("Cobertura das colunas")
cobertura = pd.DataFrame(
    [
        {
            "coluna": c,
            "preenchido": int(df[c].notna().sum()),
            "% preenchido": f"{100 * df[c].notna().sum() / len(df):.1f}%",
        }
        for c in df.columns
        if c not in {"regiao_codigo", "regiao_nome", "ano", "mes", "data"}
    ]
).sort_values("preenchido", ascending=False)
st.dataframe(cobertura, use_container_width=True, hide_index=True)

# ---- Filtro ----
st.divider()
regioes = sorted(df["regiao_nome"].unique())
sel_regioes = st.multiselect(
    "Regiões", regioes, default=regioes,
    help="Selecione uma ou mais regiões",
)
df_f = df[df["regiao_nome"].isin(sel_regioes)] if sel_regioes else df

# ---- IBCR-CE (controle estadual) ----
if "ibcr_ce" in df.columns and df["ibcr_ce"].notna().any():
    st.subheader("IBCR-CE — Índice de Atividade Econômica do Ceará (controle estadual)")
    ibcr_serie = (
        df_f.dropna(subset=["ibcr_ce"])
        .drop_duplicates(subset=["data"])
        .sort_values("data")
    )
    fig = px.line(
        ibcr_serie, x="data", y="ibcr_ce",
        labels={"data": "", "ibcr_ce": "IBCR-CE (ajuste sazonal)"},
    )
    fig.update_layout(hovermode="x unified", height=350, margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

# ---- Investimento total CE estimado (FBCF × share) ----
if "invest_total_ce_r_2010_mi" in df.columns and df["invest_total_ce_r_2010_mi"].notna().any():
    st.subheader("Investimento total CE estimado")
    st.caption(
        "Hipótese Prof. Paulo (abr/2026): "
        "FBCF Brasil mensal × share PIB CE/Brasil (~2,2%). "
        "Mesmo valor replicado nas 14 regiões — para regionalizar, "
        "multiplicar pelo share PIB regional dentro do CE."
    )
    inv_serie = (
        df_f.dropna(subset=["invest_total_ce_r_2010_mi"])
        .drop_duplicates(subset=["data"])
        .sort_values("data")
    )
    fig = px.area(
        inv_serie, x="data", y="invest_total_ce_r_2010_mi",
        labels={
            "data": "",
            "invest_total_ce_r_2010_mi": "R$ milhões (preços 2010)",
        },
    )
    fig.update_layout(hovermode="x unified", height=350, margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

# ---- SIOF (anual) ----
if "siof_anual_pago" in df.columns and df["siof_anual_pago"].notna().any():
    st.subheader("SIOF-CE — Investimento estadual em obras (anual, R$)")
    siof_anual = (
        df_f.dropna(subset=["siof_anual_pago"])
        .groupby(["ano", "regiao_nome"], as_index=False)["siof_anual_pago"]
        .first()
    )
    fig = px.bar(
        siof_anual, x="ano", y="siof_anual_pago", color="regiao_nome",
        labels={"siof_anual_pago": "Pago (R$)", "regiao_nome": "Região"},
    )
    fig.update_layout(barmode="stack", height=400, margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(
        "SIOF-CE histórico ainda não coletado para 2015-2025 — "
        "atualmente só 2026 está em `processed/execucao_orcamentaria/ce/`."
    )

# ---- Bolsa Família (se disponível) ----
if "bf_valor_total" in df.columns and df["bf_valor_total"].notna().any():
    st.subheader("Bolsa Família / Auxílio Brasil / Novo BF — total mensal por região")
    bf = (
        df_f.dropna(subset=["bf_valor_total"])
        .groupby(["data", "regiao_nome"], as_index=False)["bf_valor_total"]
        .sum()
    )
    fig = px.line(
        bf, x="data", y="bf_valor_total", color="regiao_nome",
        labels={"bf_valor_total": "R$ pago", "regiao_nome": "Região"},
    )
    fig.update_layout(hovermode="x unified", height=400, margin=dict(t=10))
    st.plotly_chart(fig, use_container_width=True)

# ---- Transferências constitucionais STN (se disponível) ----
cols_transf = [c for c in df.columns if c.startswith("transf_fed_")]
if cols_transf and df[cols_transf].notna().any().any():
    st.subheader("Transferências constitucionais federais (STN) — por destino")
    transf = df_f.groupby(["data"], as_index=False)[cols_transf].sum()
    fig = go.Figure()
    for c in cols_transf:
        if c.endswith("_total"):
            continue
        fig.add_trace(go.Scatter(
            x=transf["data"], y=transf[c],
            name=c.replace("transf_fed_", "").replace("_dest", ""),
            mode="lines",
        ))
    fig.update_layout(hovermode="x unified", height=400, margin=dict(t=10), yaxis_title="R$ recebido")
    st.plotly_chart(fig, use_container_width=True)

# ---- Tabela bruta ----
with st.expander("Tabela bruta (1.848 linhas no painel completo)"):
    st.dataframe(df_f.head(500), use_container_width=True, hide_index=True)
    st.download_button(
        "Baixar painel completo (CSV)",
        df.to_csv(index=False).encode("utf-8"),
        file_name="painel_regional_ce_mensal.csv",
        mime="text/csv",
    )

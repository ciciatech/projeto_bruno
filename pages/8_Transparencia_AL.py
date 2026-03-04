import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="Transparencia AL", layout="wide")
st.title("Transparencia AL — Execucao Orcamentaria de Alagoas")

PROC = PROCESSED_DIR
path = PROC / "transparencia_al.parquet"
if not path.exists():
    st.warning("Dados de AL nao encontrados. Execute o ETL.")
    st.stop()

df = pd.read_parquet(path)
df = df.dropna(subset=["unidade_gestora"])

# --- Filtros ---
col1, col2 = st.columns(2)
with col1:
    anos = sorted(df["ano"].dropna().unique())
    sel_anos = st.slider(
        "Periodo", int(min(anos)), int(max(anos)),
        (int(min(anos)), int(max(anos))), key="al_ano",
    )
with col2:
    ugs = sorted(df["unidade_gestora"].unique())
    ultimo_ano = df[df["ano"] == max(anos)]
    top10 = ultimo_ano.nlargest(10, "empenhado")["unidade_gestora"].tolist()
    sel_ugs = st.multiselect(
        "Unidades Gestoras", ugs, default=top10, key="al_ug",
    )

mask = df["ano"].between(*sel_anos) & df["unidade_gestora"].isin(sel_ugs)
filt = df[mask]

# --- Metricas do ultimo ano selecionado ---
ult = filt[filt["ano"] == sel_anos[1]]
if not ult.empty:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Dotacao Atualizada", f"R$ {ult['dotacao_atualizada'].sum() / 1e9:.1f} bi")
    c2.metric("Empenhado", f"R$ {ult['empenhado'].sum() / 1e9:.1f} bi")
    c3.metric("Pago", f"R$ {ult['pago'].sum() / 1e9:.1f} bi")
    dotacao_total = ult["dotacao_atualizada"].sum()
    exec_pct = ult["pago"].sum() / dotacao_total * 100 if dotacao_total > 0 else 0
    c4.metric("Execucao (Pago/Dotacao)", f"{exec_pct:.1f}%")

st.divider()

# --- Evolucao anual agregada ---
st.subheader("Evolucao Anual — Dotacao, Empenhado e Pago")
evol = filt.groupby("ano", as_index=False).agg(
    dotacao=("dotacao_atualizada", "sum"),
    empenhado=("empenhado", "sum"),
    pago=("pago", "sum"),
)
fig = go.Figure()
fig.add_trace(go.Bar(x=evol["ano"], y=evol["dotacao"], name="Dotacao Atualizada", marker_color="#bdc3c7"))
fig.add_trace(go.Bar(x=evol["ano"], y=evol["empenhado"], name="Empenhado", marker_color="#3498db"))
fig.add_trace(go.Bar(x=evol["ano"], y=evol["pago"], name="Pago", marker_color="#2ecc71"))
fig.update_layout(
    barmode="group", hovermode="x unified", height=400, margin=dict(t=10),
    yaxis_title="R$", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
    xaxis_dtick=1,
)
st.plotly_chart(fig, use_container_width=True)

# --- Ranking por UG (ultimo ano selecionado) ---
st.subheader(f"Ranking por Unidade Gestora — {sel_anos[1]}")
if not ult.empty:
    rank = ult[["unidade_gestora", "empenhado", "pago"]].copy()
    rank.sort_values("empenhado", ascending=True, inplace=True)
    fig = px.bar(
        rank, x="empenhado", y="unidade_gestora", orientation="h",
        labels={"empenhado": "R$ Empenhado", "unidade_gestora": ""},
        text_auto=".3s",
    )
    fig.update_layout(
        height=max(400, len(rank) * 25), margin=dict(t=10, l=300),
        xaxis_tickprefix="R$ ", xaxis_tickformat=",.0f",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Sem dados para o ano selecionado.")

# --- Taxa de execucao por UG ---
st.subheader(f"Taxa de Execucao (% Pago/Dotacao) — {sel_anos[1]}")
if not ult.empty:
    exec_df = ult[["unidade_gestora", "dotacao_atualizada", "pago"]].copy()
    exec_df["pct_pago"] = (exec_df["pago"] / exec_df["dotacao_atualizada"] * 100).fillna(0).clip(0, 200)
    exec_df.sort_values("pct_pago", ascending=True, inplace=True)
    fig = px.bar(
        exec_df, x="pct_pago", y="unidade_gestora", orientation="h",
        labels={"pct_pago": "% Pago", "unidade_gestora": ""},
        text_auto=".1f",
        color="pct_pago",
        color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
        range_color=[50, 100],
    )
    fig.update_layout(
        height=max(400, len(exec_df) * 25), margin=dict(t=10, l=300),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Distribuicao por funcao de governo ---
st.subheader(f"Distribuicao por Funcao de Governo — {sel_anos[1]}")
ano_funcao = df[(df["ano"] == sel_anos[1]) & df["unidade_gestora"].isin(sel_ugs)]
if not ano_funcao.empty and "funcao" in ano_funcao.columns:
    func_agg = ano_funcao.groupby("funcao", as_index=False)["empenhado"].sum()
    func_agg.sort_values("empenhado", ascending=True, inplace=True)
    fig = px.bar(
        func_agg, x="empenhado", y="funcao", orientation="h",
        labels={"empenhado": "R$ Empenhado", "funcao": ""},
        text_auto=".3s",
        color_discrete_sequence=["#8e44ad"],
    )
    fig.update_layout(
        height=max(400, len(func_agg) * 25), margin=dict(t=10, l=250),
        xaxis_tickprefix="R$ ", xaxis_tickformat=",.0f",
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Evolucao por UG selecionada ---
st.subheader("Evolucao por Unidade Gestora")
sel_detalhe = st.selectbox(
    "Unidade Gestora", sorted(filt["unidade_gestora"].unique()), key="al_det",
)
det = filt[filt["unidade_gestora"] == sel_detalhe]
if not det.empty:
    det_agg = det.groupby("ano", as_index=False).agg(
        dotacao=("dotacao_atualizada", "sum"),
        empenhado=("empenhado", "sum"),
        pago=("pago", "sum"),
    )
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=det_agg["ano"], y=det_agg["dotacao"], name="Dotacao", line=dict(dash="dot", color="#bdc3c7"),
    ))
    fig.add_trace(go.Scatter(
        x=det_agg["ano"], y=det_agg["empenhado"], name="Empenhado", line=dict(width=2, color="#3498db"),
    ))
    fig.add_trace(go.Scatter(
        x=det_agg["ano"], y=det_agg["pago"], name="Pago", line=dict(width=2, color="#2ecc71"),
    ))
    fig.update_layout(
        hovermode="x unified", height=400, margin=dict(t=10),
        yaxis_title="R$", yaxis_tickprefix="R$ ", yaxis_tickformat=",.0f",
        xaxis_dtick=1,
    )
    st.plotly_chart(fig, use_container_width=True)

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="SIOF-CE", layout="wide")
st.title("SIOF — Execução Orçamentária do Ceará")

PROC = PROCESSED_DIR
path = PROC / "siof_ce.parquet"
if not path.exists():
    st.warning("Dados do SIOF-CE não encontrados. Execute o ETL.")
    st.stop()

df = pd.read_parquet(path)

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
    # Top 10 por empenhado no último ano disponível
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

import streamlit as st
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from pipeline.config import PROCESSED_DIR

st.set_page_config(page_title="PI - Despesas (API)", layout="wide")

st.title("Execução Orçamentária - Piauí (API)")
st.markdown("Dados diretos da **API do Portal da Transparência do PI**. Fonte: [transparencia2.pi.gov.br](https://transparencia2.pi.gov.br/)")


@st.cache_data(ttl=3600)
def load_data():
    path = PROCESSED_DIR / "transparencia_pi.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    # Preencher NAs
    df["empenhado"] = pd.to_numeric(df["empenhado"], errors="coerce").fillna(0)
    df["liquidado"] = pd.to_numeric(df["liquidado"], errors="coerce").fillna(0)
    df["pago"] = pd.to_numeric(df["pago"], errors="coerce").fillna(0)
    df["dotacao_atualizada"] = pd.to_numeric(df["dotacao_atualizada"], errors="coerce").fillna(0)
    return df


df = load_data()

if df is None or df.empty:
    st.warning("Dados do PI ainda não processados (arquivo missing). Execute o ETL e tente novamente.")
    st.stop()


# == Filtros ==
st.sidebar.header("Filtros Piauí")
anos_disp = sorted(df["ano"].dropna().unique(), reverse=True)
selecao_ano = st.sidebar.multiselect("Ano", options=anos_disp, default=[anos_disp[0]] if anos_disp else [])
if selecao_ano:
    df = df[df["ano"].isin(selecao_ano)]

if "unidade_gestora" in df.columns:
    ugs = sorted(df["unidade_gestora"].dropna().unique())
    sel_ug = st.sidebar.multiselect("Unidade Gestora", options=ugs)
    if sel_ug:
        df = df[df["unidade_gestora"].isin(sel_ug)]

if "funcao" in df.columns:
    funcs = sorted(df["funcao"].dropna().unique())
    sel_func = st.sidebar.multiselect("Função", options=funcs)
    if sel_func:
        df = df[df["funcao"].isin(sel_func)]

st.markdown("---")

total_empenhado = df["empenhado"].sum()
total_pago = df["pago"].sum()
total_liquidado = df["liquidado"].sum()

# Layout em colunas para os totais
c1, c2, c3 = st.columns(3)
c1.metric("Total Empenhado", f"R$ {total_empenhado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
c2.metric("Total Liquidado", f"R$ {total_liquidado:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
c3.metric("Total Pago", f"R$ {total_pago:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["Top Despesas por UG", "Top Credores/Favorecidos", "Série Temporal/Datas"])

with tab1:
    st.subheader("Top 15 Unidades Gestoras")
    if "unidade_gestora" in df.columns:
        df_ug = df.groupby("unidade_gestora", as_index=False)[["empenhado", "pago"]].sum()
        df_ug = df_ug.sort_values("empenhado", ascending=False).head(15)

        fig = px.bar(
            df_ug, 
            y="unidade_gestora", 
            x=["empenhado", "pago"], 
            orientation="h",
            barmode="group",
            labels={"value": "Valor R$", "variable": "Fase", "unidade_gestora": "Unidade Gestora"},
            title="Empenhado vs Pago por Órgão/Unidade Gestora"
        )
        fig.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_ug.style.format({"empenhado": "R$ {:,.2f}", "pago": "R$ {:,.2f}"}), use_container_width=True)
        
with tab2:
    st.subheader("Maiores Despesas por Favorecido (Credor)")
    if "favorecido" in df.columns:
        df_cred = df.groupby("favorecido", as_index=False)[["empenhado", "pago"]].sum()
        df_cred = df_cred.sort_values("empenhado", ascending=False).head(20)
        
        st.dataframe(df_cred.style.format({"empenhado": "R$ {:,.2f}", "pago": "R$ {:,.2f}"}), use_container_width=True)

with tab3:
    st.subheader("Ritmo de Execução - Mensal")
    if "data" in df.columns:
        df_temporal = df.copy()
        df_temporal["data_real"] = pd.to_datetime(df_temporal["data"], errors="coerce")
        df_temporal = df_temporal.dropna(subset=["data_real"])
        df_temporal["mes_ano"] = df_temporal["data_real"].dt.to_period("M").astype(str)
        
        df_mes = df_temporal.groupby("mes_ano", as_index=False)[["empenhado", "pago"]].sum().sort_values("mes_ano")
        
        fig_time = px.line(
            df_mes, 
            x="mes_ano", 
            y=["empenhado", "pago"],
            markers=True,
            title="Evolução Temporal da Despesa",
            labels={"value": "Valor R$", "variable": "Fase", "mes_ano": "Mês/Ano"}
        )
        st.plotly_chart(fig_time, use_container_width=True)

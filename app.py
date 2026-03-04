import streamlit as st
import pandas as pd

from pipeline.config import PROCESSED_DIR

st.set_page_config(
    page_title="Dados Nordeste - Tese DESP/UFC",
    page_icon="📊",
    layout="wide",
)

PROC = PROCESSED_DIR

st.title("Dados Públicos — Nordeste (2015–2025)")
st.markdown("**Tese DESP/UFC** — Impactos do Crédito no Crescimento Econômico do Nordeste")

st.divider()

datasets = {
    "BACEN Indicadores": ("bacen.parquet", "Séries econômicas: IBCR, SELIC, IPCA, crédito, inadimplência"),
    "Bolsa Família": ("bolsa_familia.parquet", "Novo Bolsa Família: capitais do NE (2024–2026)"),
    "SICONFI RREO": ("rreo_resumo.parquet", "Receitas e despesas orçamentárias dos 9 estados"),
    "SICONFI RGF": ("rgf_resumo.parquet", "Gestão fiscal: pessoal, dívida consolidada"),
    "SICONFI DCA": ("dca_resumo.parquet", "Balanço patrimonial: ativo, passivo, patrimônio líquido"),
    "Transferências": ("transferencias.parquet", "Transferências constitucionais: FPE, FUNDEB e outras"),
    "SIOF-CE": ("siof_ce.parquet", "Execução orçamentária do Ceará: dotação, empenho e pagamento (2015–2026)"),
    "Transparência AL": ("transparencia_al.parquet", "Execução orçamentária de Alagoas: dotação, empenho e pagamento (2015–2025)"),
}

cols = st.columns(3)
for i, (nome, (arquivo, desc)) in enumerate(datasets.items()):
    path = PROC / arquivo
    with cols[i % 3]:
        if path.exists():
            df = pd.read_parquet(path)
            st.metric(label=nome, value=f"{len(df):,} registros")
            st.caption(desc)
        else:
            st.metric(label=nome, value="—")
            st.caption("Execute o ETL primeiro.")

st.divider()

st.subheader("Sobre")
st.markdown("""
- **Período**: 2015 a 2025 (BACEN, SICONFI, SIOF) / 2024 a 2026 (Bolsa Família)
- **Região**: Nordeste — 9 estados (AL, BA, CE, MA, PB, PE, PI, RN, SE)
- **Fontes**: BACEN-SGS, SICONFI/STN, Portal da Transparência, SIOF-CE (SEPLAG/CE)

Use o menu lateral para navegar entre as páginas de cada fonte de dados.
""")

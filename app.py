import streamlit as st
import pandas as pd
from pathlib import Path

from pipeline.config import PROCESSED_DIR

st.set_page_config(
    page_title="Dados Nordeste - Tese DESP/UFC",
    page_icon="📊",
    layout="wide",
)

PROC = PROCESSED_DIR


def _contar_registros(caminhos: list[str]) -> int | None:
    """Tenta ler o primeiro CSV encontrado e retorna o número de registros."""
    for rel in caminhos:
        path = PROC / rel
        if path.exists():
            return len(pd.read_csv(path))
    return None


st.title("Dados Públicos — Nordeste (2015–2025)")
st.markdown("**Tese DESP/UFC** — Impactos do Crédito no Crescimento Econômico do Nordeste")

st.divider()

datasets = {
    "BACEN Indicadores": {
        "caminhos": ["bacen/nacional/bacen.csv", "bacen.parquet"],
        "desc": "Séries econômicas: IBCR, SELIC, IPCA, crédito, inadimplência",
    },
    "SICONFI RREO": {
        "caminhos": ["siconfi_rreo/ce/rreo_resumo.csv", "rreo_resumo.parquet"],
        "desc": "Receitas e despesas orçamentárias dos 9 estados",
    },
    "SICONFI RGF": {
        "caminhos": ["siconfi_rgf/ce/rgf_resumo.csv", "rgf_resumo.parquet"],
        "desc": "Gestão fiscal: pessoal, dívida consolidada",
    },
    "SICONFI DCA": {
        "caminhos": ["siconfi_dca/ce/dca_resumo.csv", "dca_resumo.parquet"],
        "desc": "Balanço patrimonial: ativo, passivo, patrimônio líquido",
    },
    "Transferências": {
        "caminhos": ["transferencias/ce/transferencias.csv", "transferencias.parquet"],
        "desc": "Transferências constitucionais: FPE, FUNDEB e outras",
    },
    "SIOF-CE Obras": {
        "caminhos": ["execucao_orcamentaria/ce/siof_obras_secretaria.csv", "siof_ce.parquet"],
        "desc": "Obras e Instalações do Ceará (elem. 51) por secretaria (2015–2026)",
    },
    "CAGED": {
        "caminhos": ["caged/nordeste/caged_saldo_mensal.csv"],
        "desc": "Saldo mensal de empregos formais — Nordeste",
    },
    "RAIS": {
        "caminhos": ["rais/nordeste/rais_vinculos.csv"],
        "desc": "Vínculos formais ativos — Nordeste",
    },
}

cols = st.columns(3)
for i, (nome, info) in enumerate(datasets.items()):
    with cols[i % 3]:
        n = _contar_registros(info["caminhos"])
        if n is not None:
            st.metric(label=nome, value=f"{n:,} registros")
            st.caption(info["desc"])
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

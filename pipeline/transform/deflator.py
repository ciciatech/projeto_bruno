"""
Deflator IPCA com base fixa (padrão dez/2025), validado contra a planilha do Prof. Paulo.

Convenção — reproduz a aba "Índice de inflação" da planilha
"Dados Regionais - CC SEFAZ e Tese Bruno-2.xlsx" com erro relativo máximo da
ordem de 2e-15 (apenas ruído de ponto flutuante)::

    deflator(ano, bimestre) = I(mes_base) / I(último mês do bimestre)

onde ``I(m)`` é o índice IPCA acumulado fim-de-mês, ``I(m) = prod_{k<=m} (1 + ipca_k/100)``,
construído a partir da série mensal SGS 433 do BACEN. O bimestre ``b`` cobre os
meses ``(2b-1, 2b)`` e o mês de referência é o ÚLTIMO (``2b``) — NÃO se usa
média dos dois meses (convenções alternativas erram 0,5%–1,35% frente à
planilha). Como o deflator é uma razão de índices, a base do ``cumprod`` é
irrelevante.

Uso: ``valor_real = valor_nominal × deflator`` → R$ constantes do mês-base
(padrão dez/2025). No bimestre/mês-base o deflator vale exatamente 1.0.

A base é PINADA explicitamente (padrão ``"2025-12"``), ao contrário do deflator
legado de ``preparacao_modelo.py``, cuja base é a última observação disponível
do IPCA (rolante) — se a coleta passar a incluir jan/2026+, o legado desliza de
escala; este módulo não.

Fonte do IPCA mensal, em ordem de precedência:

1. cache local ``pipeline/data/ipca_433_cache.csv`` (offline; usado pelos testes);
2. CSVs já coletados pelo pipeline (``raw/bacen/nacional/bacen_sgs_wide.csv``
   ou ``processed/bacen/nacional/bacen.csv``);
3. download da API SGS do BACEN (série 433) via ``safe_request`` — só ocorre se
   o cache não existir e nenhum CSV local estiver disponível.

Nos casos 2 e 3 o resultado é salvo no cache (mesmo padrão do cache IBGE de
``pipeline/regioes_ce.py``: baixa/deriva uma vez, lê do disco depois).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import PROCESSED_DIR, RAW_DIR
from pipeline.utils import safe_request

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
IPCA_CACHE_CSV = DATA_DIR / "ipca_433_cache.csv"
SGS_433_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"

#: Mês-base padrão do deflator (R$ constantes de dez/2025), conforme planilha
#: do Prof. Paulo. Parametrizável (ex.: "2024-12") enquanto a confirmação
#: definitiva da base estiver pendente.
BASE_PADRAO = "2025-12"


def _normalizar_mes(serie: pd.Series) -> pd.Series:
    """Converte datas para o primeiro dia do mês (timestamp mensal canônico)."""
    return pd.to_datetime(serie).dt.to_period("M").dt.to_timestamp()


def _baixar_ipca_sgs(data_inicio: str = "01/01/2015") -> pd.DataFrame:
    """Baixa a série 433 (IPCA mensal, var. %) da API SGS do BACEN.

    Retorna DataFrame com colunas ``data`` (timestamp mensal) e ``ipca_mensal``.
    Levanta ``RuntimeError`` se a API não responder.
    """
    logger.info("Baixando IPCA mensal (SGS 433) da API do BACEN...")
    resp = safe_request(SGS_433_URL, params={"dataInicial": data_inicio})
    if resp is None:
        raise RuntimeError("Falha ao baixar IPCA (SGS 433) da API do BACEN.")
    df = pd.DataFrame(resp.json())
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["ipca_mensal"] = pd.to_numeric(df["valor"], errors="coerce")
    return df[["data", "ipca_mensal"]]


def _carregar_ipca_local() -> pd.DataFrame | None:
    """Tenta carregar o IPCA mensal dos CSVs já coletados pelo pipeline.

    Mesmos candidatos de ``preparacao_modelo._carregar_ipca``; retorna ``None``
    se nenhum arquivo existir ou nenhuma coluna IPCA for encontrada.
    """
    candidatos = [
        RAW_DIR / "bacen" / "nacional" / "bacen_sgs_wide.csv",
        PROCESSED_DIR / "bacen" / "nacional" / "bacen.csv",
    ]
    for path in candidatos:
        if not path.exists():
            continue
        df = pd.read_csv(path, low_memory=False)
        col_ipca = "ipca_mensal" if "ipca_mensal" in df.columns else next(
            (c for c in df.columns if "ipca" in c.lower()), None
        )
        if col_ipca is None:
            continue
        out = df[["data", col_ipca]].rename(columns={col_ipca: "ipca_mensal"})
        logger.info(f"IPCA mensal carregado de {path}")
        return out
    return None


def obter_ipca_mensal() -> pd.DataFrame:
    """Obtém a série mensal do IPCA (SGS 433, variação % a.m.).

    Ordem de precedência: cache ``pipeline/data/ipca_433_cache.csv`` →
    CSVs locais do pipeline → download da API SGS (com ``safe_request``).
    Quando a fonte não é o cache, o cache é (re)gerado para uso offline.

    Retorna DataFrame ordenado com colunas ``data`` (timestamp no 1º dia do
    mês) e ``ipca_mensal`` (float, sem NaN).
    """
    if IPCA_CACHE_CSV.exists():
        df = pd.read_csv(IPCA_CACHE_CSV)
    else:
        df = _carregar_ipca_local()
        if df is None:
            df = _baixar_ipca_sgs()
        df = df.dropna(subset=["ipca_mensal"]).copy()
        df["data"] = _normalizar_mes(df["data"])
        df = df.sort_values("data").reset_index(drop=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(IPCA_CACHE_CSV, index=False)
        logger.info(f"Cache IPCA salvo: {IPCA_CACHE_CSV} ({len(df)} meses)")

    df["data"] = _normalizar_mes(df["data"])
    df["ipca_mensal"] = pd.to_numeric(df["ipca_mensal"], errors="coerce")
    return (
        df.dropna(subset=["ipca_mensal"])
        .sort_values("data")
        .reset_index(drop=True)[["data", "ipca_mensal"]]
    )


def indice_ipca_acumulado(ipca: pd.DataFrame | None = None) -> pd.Series:
    """Constrói o índice IPCA acumulado fim-de-mês: ``I(m) = prod(1 + v_k/100)``.

    ``ipca``: DataFrame opcional com colunas ``data`` e ``ipca_mensal``
    (default: ``obter_ipca_mensal()``). Retorna ``pd.Series`` indexada pelo
    timestamp mensal. A escala (base do cumprod) é irrelevante para o deflator,
    que é uma razão de índices.
    """
    if ipca is None:
        ipca = obter_ipca_mensal()
    serie = ipca.set_index("data")["ipca_mensal"].sort_index()
    return (serie / 100.0 + 1.0).cumprod()


def _indice_no_mes_base(indice: pd.Series, base: str) -> float:
    """Valor do índice acumulado no mês-base; valida formato e disponibilidade."""
    try:
        base_ts = pd.Period(base, freq="M").to_timestamp()
    except Exception as exc:
        raise ValueError(f"Base inválida: {base!r} (esperado 'YYYY-MM').") from exc
    if base_ts not in indice.index:
        raise ValueError(
            f"Mês-base {base!r} fora da série IPCA disponível "
            f"({indice.index.min():%Y-%m} a {indice.index.max():%Y-%m})."
        )
    return float(indice.loc[base_ts])


def deflator_bimestral(
    base: str = BASE_PADRAO, indice: pd.Series | None = None
) -> pd.DataFrame:
    """Deflator bimestral IPCA com base fixa (convenção do Prof. Paulo).

    ``deflator(ano, b) = I(base) / I(último mês do bimestre)``, com
    último mês = ``2b`` (b1=fev, b2=abr, ..., b6=dez). Só entram bimestres
    completos (cujo mês final existe na série IPCA).

    ``base``: mês-base no formato ``"YYYY-MM"`` (padrão ``"2025-12"``).
    ``indice``: índice acumulado opcional (default: ``indice_ipca_acumulado()``).

    Retorna DataFrame com colunas ``(ano, bimestre, deflator)``, ordenado;
    ``deflator == 1.0`` no bimestre que termina no mês-base.
    """
    if indice is None:
        indice = indice_ipca_acumulado()
    i_base = _indice_no_mes_base(indice, base)

    meses_finais = indice[indice.index.month % 2 == 0]
    out = pd.DataFrame(
        {
            "ano": meses_finais.index.year,
            "bimestre": meses_finais.index.month // 2,
            "deflator": i_base / meses_finais.to_numpy(),
        }
    )
    return out.sort_values(["ano", "bimestre"]).reset_index(drop=True)


def deflator_mensal(
    base: str = BASE_PADRAO, indice: pd.Series | None = None
) -> pd.DataFrame:
    """Deflator mensal IPCA com base fixa — mesmo princípio do bimestral.

    ``deflator(ano, mes) = I(base) / I(mes)`` com o índice acumulado
    fim-de-mês. Para uso no painel mensal (T27 e afins).

    Retorna DataFrame com colunas ``(ano, mes, deflator)``, ordenado;
    ``deflator == 1.0`` no mês-base.
    """
    if indice is None:
        indice = indice_ipca_acumulado()
    i_base = _indice_no_mes_base(indice, base)

    out = pd.DataFrame(
        {
            "ano": indice.index.year,
            "mes": indice.index.month,
            "deflator": i_base / indice.to_numpy(),
        }
    )
    return out.sort_values(["ano", "mes"]).reset_index(drop=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    df_bim = deflator_bimestral()
    print(f"\nDeflator bimestral base {BASE_PADRAO}: {len(df_bim)} bimestres")
    print(df_bim.head(3).to_string(index=False))
    print("...")
    print(df_bim.tail(3).to_string(index=False))

"""
Coletor ESTBAN — Estatística Bancária Mensal por Município (BCB).

Filtra crédito do Banco do Nordeste (BNB) nos municípios cearenses, gerando
um CSV consolidado (cod_ibge × ano × mês × valor) para o painel regional CE.

## Origem dos dados

O ESTBAN foi reformulado pela IN BCB 502/2024: deixou de ter download massivo
público pela URL legada ``www4.bcb.gov.br/fis/cosif/estban.asp`` e passou a ser
consultável via Sistema Cosif (UI). Não há mais URL programática estável para
todos os meses históricos. O coletor portanto funciona como **adaptador
flexível** sobre arquivos baixados de uma destas fontes:

1. Sistema Cosif: https://www.bcb.gov.br/estabilidadefinanceira/sistemacosif
2. Base dos Dados (BigQuery): ``basedosdados.br_bcb_estban.municipio`` —
   exporte filtrando ``id_uf = '23'`` para um CSV/parquet por mês.
3. Mirror histórico que a equipe de pesquisa já tenha baixado.

Coloque os arquivos brutos em ``dados_nordeste/raw/estban/`` (qualquer
subdiretório) e este coletor cuida do parsing.

## Formatos suportados

- **CSV ESTBAN clássico** (``;`` como delimitador, encoding latin-1):
  Colunas: ``CNPJ_PARCIAL;NOME_INSTITUICAO;CODMUN_IBGE;NOME_MUNICIPIO;UF;
  CD_AGENCIA;NOME_AGENCIA;CD_VERBETE_ESTBAN;DATA_BASE;SALDO``
- **CSV / parquet do Base dos Dados** (snake_case): ``ano, mes, sigla_uf,
  id_municipio, cnpj, cnpj_basico, id_verbete, valor`` ou similar.
- **CSV genérico**: detectamos colunas por nome (case/acento insensitive).

## Filtros

- **Banco do Nordeste**: detectado por nome contendo "BANCO DO NORDESTE" ou
  CNPJ raiz ``07237373`` (ISPB do BNB).
- **Ceará**: ``UF == 'CE'`` ou ``COD_IBGE`` começando com ``23``.

## Verbetes COSIF de crédito

Por padrão somamos os principais verbetes de operações de crédito ativas:
- 160: Empréstimos e Títulos Descontados
- 161: Financiamentos
- 162: Financiamentos Rurais e Agroindustriais
- 163: Financiamentos Imobiliários
- 169: Outras Operações de Crédito

A seleção é configurável via ``VERBETES_CREDITO``. Saldo total de crédito é
gerado em uma coluna agregada ``credito_bnb_total`` para cada município/mês.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import pandas as pd

from pipeline.config import COD_IBGE_CE, RAW_DIR
from pipeline.regioes_ce import get_codigos_municipios_ce
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)


ISPB_BNB_RAIZ = "07237373"  # CNPJ raiz do Banco do Nordeste

VERBETES_CREDITO = {
    "160": "emprestimos_titulos_descontados",
    "161": "financiamentos",
    "162": "financiamentos_rurais",
    "163": "financiamentos_imobiliarios",
    "169": "outras_operacoes_credito",
}


def _slug(s: str) -> str:
    """Normaliza nome de coluna: minúsculas, sem acentos, sem espaços."""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _detectar_colunas(df: pd.DataFrame) -> dict[str, str]:
    """Mapeia colunas do DataFrame de entrada para nomes canônicos."""
    canon: dict[str, str] = {}
    for col in df.columns:
        s = _slug(col)
        if s in {"data_base", "data", "ano_mes", "ref"} and "data_base" not in canon:
            canon["data_base"] = col
        elif s in {"ano"} and "ano" not in canon:
            canon["ano"] = col
        elif s in {"mes"} and "mes" not in canon:
            canon["mes"] = col
        elif s in {"cnpj", "cnpj_parcial", "cnpj_basico"} and "cnpj" not in canon:
            canon["cnpj"] = col
        elif s in {"nome_instituicao", "instituicao"} and "instituicao" not in canon:
            canon["instituicao"] = col
        elif s in {"uf", "sigla_uf"} and "uf" not in canon:
            canon["uf"] = col
        elif s in {
            "codmun_ibge",
            "id_municipio",
            "cod_ibge",
            "cod_municipio",
            "codigo_ibge",
            "codigo_municipio",
        } and "cod_ibge" not in canon:
            canon["cod_ibge"] = col
        elif s in {
            "cd_verbete_estban",
            "id_verbete",
            "verbete",
            "codigo_verbete",
            "cod_verbete",
        } and "verbete" not in canon:
            canon["verbete"] = col
        elif s in {"saldo", "valor"} and "valor" not in canon:
            canon["valor"] = col
    return canon


def _ler_arquivo(path: Path) -> pd.DataFrame:
    """Lê um único arquivo ESTBAN (CSV/parquet), retornando DataFrame raw."""
    suf = path.suffix.lower()
    if suf in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suf in {".csv", ".txt"}:
        # tentar primeiro com ; (formato BCB clássico) e latin-1
        for sep in [";", ","]:
            for enc in ["latin-1", "utf-8", "utf-8-sig"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, low_memory=False)
                    if df.shape[1] > 1:
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue
        raise ValueError(f"Não foi possível ler {path} como CSV.")
    if suf in {".gz"}:
        return pd.read_csv(path, sep=";", encoding="latin-1", dtype=str,
                           compression="gzip", low_memory=False)
    raise ValueError(f"Formato não suportado: {path.suffix}")


def _normalizar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica detecção de colunas e normaliza tipos/encoding."""
    canon = _detectar_colunas(df)
    obrig = ["cod_ibge", "verbete", "valor"]
    falt = [c for c in obrig if c not in canon]
    if falt:
        raise ValueError(
            f"Colunas obrigatórias não encontradas: {falt}. "
            f"Detectadas: {list(canon.keys())} | Originais: {list(df.columns)}"
        )

    out = pd.DataFrame()
    out["cod_ibge"] = df[canon["cod_ibge"]].astype(str).str.replace(r"\D", "", regex=True)
    out["verbete"] = df[canon["verbete"]].astype(str).str.replace(r"\D", "", regex=True).str.zfill(3)
    out["valor"] = pd.to_numeric(df[canon["valor"]].astype(str).str.replace(",", "."), errors="coerce")

    if "uf" in canon:
        out["uf"] = df[canon["uf"]].astype(str).str.upper().str.strip()

    # CNPJ: extrair raiz (8 dígitos)
    if "cnpj" in canon:
        out["cnpj_raiz"] = (
            df[canon["cnpj"]].astype(str).str.replace(r"\D", "", regex=True).str[:8]
        )

    if "instituicao" in canon:
        out["instituicao"] = df[canon["instituicao"]].astype(str).str.upper().str.strip()

    # Ano/mês: tentar a partir de data_base, ou ano + mes diretos
    if "data_base" in canon:
        s = df[canon["data_base"]].astype(str).str.replace(r"\D", "", regex=True)
        # esperamos AAAAMM (6 dígitos) ou AAAAMMDD (8 dígitos)
        out["ano"] = s.str[:4].astype(int, errors="ignore")
        out["mes"] = s.str[4:6].astype(int, errors="ignore")
    elif "ano" in canon and "mes" in canon:
        out["ano"] = pd.to_numeric(df[canon["ano"]], errors="coerce").astype("Int64")
        out["mes"] = pd.to_numeric(df[canon["mes"]], errors="coerce").astype("Int64")
    else:
        raise ValueError("Não foi possível identificar ano/mês no arquivo.")

    return out


def _filtrar_ce_bnb(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica filtros: município CE + Banco do Nordeste."""
    cods_ce = set(get_codigos_municipios_ce())

    # Filtro CE: por código IBGE (preferido) ou UF (fallback)
    df["cod_ibge"] = df["cod_ibge"].astype(str)
    # ESTBAN clássico usa COD_IBGE de 6 dígitos (sem dígito verificador). IBGE oficial
    # tem 7. Aceitamos ambos: se 6 dígitos e começa com 23, é CE; se 7, batemos com a lista.
    df["_ibge7"] = df["cod_ibge"].apply(
        lambda x: x if len(x) == 7 else (x + "0" if len(x) == 6 else "")
    )
    mask_ce_por_codigo = df["_ibge7"].isin(cods_ce) | df["cod_ibge"].str.startswith(
        COD_IBGE_CE
    )
    if "uf" in df.columns:
        mask_ce = mask_ce_por_codigo | (df["uf"] == "CE")
    else:
        mask_ce = mask_ce_por_codigo
    df = df[mask_ce].copy()

    # Filtro BNB: por CNPJ raiz ou nome
    mask_bnb = pd.Series(False, index=df.index)
    if "cnpj_raiz" in df.columns:
        mask_bnb |= df["cnpj_raiz"] == ISPB_BNB_RAIZ
    if "instituicao" in df.columns:
        mask_bnb |= df["instituicao"].str.contains(
            "BANCO DO NORDESTE", regex=False, na=False
        )
    if not mask_bnb.any():
        logger.warning("Nenhum registro do BNB encontrado no arquivo.")
    return df[mask_bnb].copy()


def _agregar_municipal(df: pd.DataFrame) -> pd.DataFrame:
    """Pivota para (cod_ibge, ano, mes) × verbete e calcula total de crédito."""
    if df.empty:
        return pd.DataFrame()

    df = df[df["verbete"].isin(VERBETES_CREDITO)].copy()
    if df.empty:
        logger.warning("Nenhum verbete de crédito (160/161/162/163/169) encontrado.")
        return pd.DataFrame()

    df["valor"] = df["valor"].fillna(0)
    grp = df.groupby(["_ibge7", "ano", "mes", "verbete"], as_index=False)["valor"].sum()
    pivot = grp.pivot_table(
        index=["_ibge7", "ano", "mes"],
        columns="verbete",
        values="valor",
        fill_value=0,
        aggfunc="sum",
    ).reset_index()
    pivot.columns.name = None
    pivot = pivot.rename(columns={"_ibge7": "cod_ibge"})
    pivot = pivot.rename(columns={k: f"bnb_{v}" for k, v in VERBETES_CREDITO.items()})
    cols_credito = [f"bnb_{v}" for v in VERBETES_CREDITO.values() if f"bnb_{v}" in pivot.columns]
    pivot["credito_bnb_total"] = pivot[cols_credito].sum(axis=1)
    pivot = pivot.sort_values(["ano", "mes", "cod_ibge"]).reset_index(drop=True)
    return pivot


def coletar_de_diretorio(diretorio: Path | str | None = None) -> pd.DataFrame:
    """
    Lê todos os arquivos ESTBAN em ``diretorio`` (default
    ``dados_nordeste/raw/estban``), aplica filtros CE+BNB e agrega por município/mês.

    Retorna DataFrame consolidado e salva em
    ``dados_nordeste/processed/estban/credito_bnb_municipio_ce_mensal.csv``.
    """
    diretorio = Path(diretorio) if diretorio else (RAW_DIR / "estban")
    if not diretorio.exists():
        logger.warning(
            f"Diretório {diretorio} não existe. Crie e coloque os arquivos ESTBAN "
            "(CSV/parquet) baixados do BCB Sistema Cosif ou do Base dos Dados."
        )
        return pd.DataFrame()

    arquivos = [
        p
        for p in diretorio.rglob("*")
        if p.is_file() and p.suffix.lower() in {".csv", ".txt", ".gz", ".parquet", ".pq"}
    ]
    if not arquivos:
        logger.warning(f"Nenhum arquivo ESTBAN encontrado em {diretorio}.")
        return pd.DataFrame()

    logger.info(f"ESTBAN: lendo {len(arquivos)} arquivos de {diretorio}")
    frames = []
    for arq in arquivos:
        try:
            raw = _ler_arquivo(arq)
            norm = _normalizar(raw)
            filt = _filtrar_ce_bnb(norm)
            if not filt.empty:
                frames.append(filt)
            logger.info(f"  {arq.name}: {len(raw)} → {len(filt)} (CE+BNB)")
        except Exception as e:
            logger.error(f"  {arq.name}: erro — {e}")

    if not frames:
        logger.warning("ESTBAN: nenhum dado CE+BNB extraído.")
        return pd.DataFrame()

    bruto = pd.concat(frames, ignore_index=True)
    consolidado = _agregar_municipal(bruto)

    if not consolidado.empty:
        save_dataframe(
            consolidado,
            "credito_bnb_municipio_ce_mensal",
            subdir="processed",
            path_parts=["estban"],
        )
        logger.info(
            f"ESTBAN: consolidado {len(consolidado)} linhas "
            f"({consolidado['cod_ibge'].nunique()} municípios CE × "
            f"{consolidado.groupby(['ano','mes']).ngroups} meses)"
        )
    return consolidado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    df = coletar_de_diretorio()
    if not df.empty:
        print(df.head())
        print(f"\nTotal: {len(df)} linhas, {df['cod_ibge'].nunique()} municípios")

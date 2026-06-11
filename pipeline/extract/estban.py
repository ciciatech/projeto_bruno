"""
Coletor ESTBAN — Estatística Bancária Mensal por Município (BCB).

Baixa os arquivos mensais do ESTBAN direto do site moderno do BCB e filtra o
crédito do Banco do Nordeste (BNB) nos municípios cearenses, gerando dois CSVs:

- ``dados_nordeste/processed/estban/credito_bnb_municipio_ce_mensal.csv``
  (detalhe por verbete de crédito, município × mês)
- ``dados_nordeste/processed/estban/credito_municipio_ce_mensal.csv``
  (painel: cod_ibge, regiao_codigo, regiao_nome, ano, mes, credito_operacoes)

## Origem dos dados (descoberta 06/2026)

As URLs legadas ``www4.bcb.gov.br/fis/cosif/estban.asp`` retornam 404. A página
moderna https://www.bcb.gov.br/estatisticas/estatisticabancariamunicipios é uma
SPA que consome a API pública (sem auth) ``api/servico/sitebcb``:

1. ``conteudosite?identificador=estatistica_bancaria_estban`` → ``guidLista``
2. ``Documentos/byListGuid?tronco=...&guidLista=...&pasta=municipio`` → lista
   de ~453 arquivos (1988-07 em diante) com ``Url``/``Nome``/``Tamanho``.

Padrões de nome (não hardcodar — usamos o campo ``Url`` da API):
``<AAAAMM>_ESTBAN.ZIP`` (até 2022-12), ``202301_ESTBAN.csv`` (csv solto) e
``<AAAAMM>_ESTBAN.csv.zip`` (2023-02 em diante). ~0,9–2,7 MB/mês.

Os últimos 6 meses publicados são REVISADOS a cada publicação (~60 dias após a
data-base); para coleta incremental vale re-baixar os meses mais recentes.

## Formato dos arquivos (layout WIDE)

CSV ``;`` latin-1, com 2 linhas de preâmbulo antes do header, que começa com
``#DATA_BASE;UF;CODMUN;MUNICIPIO;CNPJ;NOME_INSTITUICAO;...;CODMUN_IBGE`` e traz
os verbetes COSIF em COLUNAS (``VERBETE_160_OPERACOES_DE_CREDITO``, ...). O
conjunto de colunas varia entre anos (66 em 2015 → 54 em 2024), mas DATA_BASE,
UF, CNPJ, NOME_INSTITUICAO, VERBETE_160_* e CODMUN_IBGE (7 dígitos) existem em
todos. ``_melt_wide`` derrete para o formato longo (verbete, saldo) antes da
normalização. Formatos longos (Base dos Dados, mirrors) seguem suportados.

## Filtros

- **Banco do Nordeste**: nome contendo "BANCO DO NORDESTE" ou "BCO DO NORDESTE"
  (grafia real dos arquivos: "BCO DO NORDESTE DO BRASIL S.A.") ou CNPJ raiz
  ``07237373``.
- **Ceará**: ``UF == 'CE'`` ou código IBGE de município CE.

## Verbetes COSIF de crédito (layout wide real)

- **160: OPERAÇÕES DE CRÉDITO — saldo TOTAL** (métrica do painel; é o que o
  Prof. Paulo pediu). Os demais são ABERTURAS do 160 e NÃO devem ser somados a
  ele (contaria em dobro):
- 161: Empréstimos e Títulos Descontados
- 162: Financiamentos
- 163: Financiamentos Rurais — Agricultura
- 167: Financiamentos Agroindustriais
- 169: Financiamentos Imobiliários

``credito_bnb_total``/``credito_operacoes`` = verbete 160 puro.

## Caveats para o painel

1. ESTBAN cobre só municípios COM agência; o BNB tem agência em ~39 dos 184
   municípios CE — município sem linha = sem agência BNB (não é crédito zero).
   O crédito é registrado no município da AGÊNCIA, não do tomador.
2. Unidade monetária: ordens de grandeza sugerem R$ correntes (soma CE/BNB
   2024-01 ≈ R$ 7,9 bi). Confirmar contra IF.data/Cosif antes de deflacionar
   para R$ dez/25 (padrão da planilha do Prof. Paulo).
"""

from __future__ import annotations

import io
import logging
import re
import time
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd

from pipeline.config import COD_IBGE_CE, PERIODO_FIM, PERIODO_INICIO, RAW_DIR
from pipeline.regioes_ce import get_codigos_municipios_ce, get_regiao_info
from pipeline.utils import safe_request, save_dataframe

logger = logging.getLogger(__name__)


ISPB_BNB_RAIZ = "07237373"  # CNPJ raiz do Banco do Nordeste

# Verbete 160 é o saldo TOTAL de operações de crédito; 161..169 são aberturas
# dele (NÃO somar com o 160 — contaria em dobro).
VERBETE_CREDITO_TOTAL = "160"
VERBETES_CREDITO = {
    "160": "operacoes_credito_total",
    "161": "emprestimos_titulos_descontados",
    "162": "financiamentos",
    "163": "financiamentos_rurais_agricultura",
    "167": "financiamentos_agroindustriais",
    "169": "financiamentos_imobiliarios",
}

# --- API do site moderno do BCB (descoberta por engenharia reversa da SPA) ---
BCB_BASE_URL = "https://www.bcb.gov.br"
BCB_CONTEUDOSITE_URL = (
    f"{BCB_BASE_URL}/api/servico/sitebcb/conteudosite"
    "?identificador=estatistica_bancaria_estban"
)
BCB_BYLISTGUID_URL = f"{BCB_BASE_URL}/api/servico/sitebcb/Documentos/byListGuid"
BCB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "*/*",
}

_RE_AAAAMM = re.compile(r"(\d{6})_ESTBAN", re.IGNORECASE)
_RE_COL_VERBETE = re.compile(r"^\s*VERBETE_(\d+)", re.IGNORECASE)


# ==============================================================================
# DOWNLOAD (API sitebcb)
# ==============================================================================


def _resolver_guid_estban() -> tuple[str, str]:
    """Resolve (tronco, guidLista) da lista de documentos ESTBAN em runtime.

    Não hardcodamos o GUID: se o BCB trocá-lo, a resolução via
    ``conteudosite`` continua funcionando.
    """
    resp = safe_request(BCB_CONTEUDOSITE_URL, headers=BCB_HEADERS)
    if resp is None:
        raise RuntimeError("Falha ao consultar conteudosite do BCB (ESTBAN).")
    conteudo = resp.json().get("conteudo") or []
    for item in conteudo:
        if item.get("guidLista"):
            return item.get("tronco", "estatisticas"), item["guidLista"]
    raise RuntimeError(f"guidLista não encontrado na resposta: {conteudo!r}")


def listar_documentos_estban(pasta: str = "municipio") -> list[dict]:
    """Lista os documentos ESTBAN disponíveis (Url, Nome, Tamanho, DataDocumento).

    ``pasta`` ∈ {"municipio", "agencia"}.
    """
    tronco, guid = _resolver_guid_estban()
    resp = safe_request(
        BCB_BYLISTGUID_URL,
        params={
            "tronco": tronco,
            "guidLista": guid,
            "ordem": "DataDocumento desc",
            "pasta": pasta,
        },
        headers=BCB_HEADERS,
    )
    if resp is None:
        raise RuntimeError("Falha ao listar documentos ESTBAN (byListGuid).")
    data = resp.json()
    docs = data.get("conteudo") if isinstance(data, dict) else data
    if not docs:
        raise RuntimeError("Lista de documentos ESTBAN veio vazia.")
    return docs


def baixar_estban_bcb(
    ano_inicio: int = PERIODO_INICIO,
    ano_fim: int = PERIODO_FIM,
    destino: Path | str | None = None,
    pasta: str = "municipio",
    sleep_s: float = 0.5,
) -> list[Path]:
    """
    Baixa os arquivos mensais do ESTBAN (``ano_inicio``..``ano_fim``) para
    ``destino`` (default ``dados_nordeste/raw/estban``). Idempotente: arquivos
    já existentes (tamanho > 0) são pulados.

    Retorna a lista de paths locais (baixados + já existentes) no período.
    """
    destino = Path(destino) if destino else (RAW_DIR / "estban")
    destino.mkdir(parents=True, exist_ok=True)

    docs = listar_documentos_estban(pasta=pasta)
    selecionados: list[tuple[str, str, str]] = []
    for doc in docs:
        url = doc.get("Url") or ""
        nome = doc.get("Nome") or Path(url).name
        m = _RE_AAAAMM.search(nome)
        if not m:
            continue
        aaaamm = m.group(1)
        if ano_inicio <= int(aaaamm[:4]) <= ano_fim:
            selecionados.append((aaaamm, nome, url))
    selecionados.sort()
    logger.info(
        f"ESTBAN: {len(selecionados)} arquivos no período {ano_inicio}-{ano_fim} "
        f"(pasta={pasta}) → {destino}"
    )

    paths: list[Path] = []
    n_baixados = n_pulados = n_erros = 0
    for aaaamm, nome, url in selecionados:
        alvo = destino / nome
        if alvo.exists() and alvo.stat().st_size > 0:
            paths.append(alvo)
            n_pulados += 1
            continue
        url_full = url if url.startswith("http") else BCB_BASE_URL + url
        resp = safe_request(url_full, headers=BCB_HEADERS)
        if resp is None or not resp.content:
            logger.error(f"  {nome}: download falhou ({url_full})")
            n_erros += 1
            continue
        tmp = Path(str(alvo) + ".part")
        tmp.write_bytes(resp.content)
        if alvo.suffix.lower() == ".zip" and not zipfile.is_zipfile(tmp):
            logger.error(f"  {nome}: conteúdo baixado não é um ZIP válido — descartado")
            tmp.unlink(missing_ok=True)
            n_erros += 1
            continue
        tmp.rename(alvo)
        paths.append(alvo)
        n_baixados += 1
        logger.info(f"  {nome}: ok ({len(resp.content) / 1e6:.2f} MB)")
        time.sleep(sleep_s)

    logger.info(
        f"ESTBAN download: {n_baixados} baixados, {n_pulados} já existiam, "
        f"{n_erros} erros."
    )
    return paths


# ==============================================================================
# PARSING
# ==============================================================================


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


def _sniffar_linha_header(texto: str) -> int | None:
    """Acha o índice da linha de header do layout wide do BCB ("#DATA_BASE;...").

    Os CSVs do site moderno têm 2 linhas de preâmbulo ("ESTBAN (Documento
    4500)..." e "Data de geracao..."); sniffamos em vez de fixar skiprows=2.
    Retorna None se o texto não está no layout wide do BCB.
    """
    for i, linha in enumerate(texto.splitlines()[:30]):
        if linha.lstrip().lstrip("\ufeff").upper().startswith(
            ("#DATA_BASE", "DATA_BASE;")
        ):
            return i
    return None


def _ler_csv_texto(texto: str) -> pd.DataFrame:
    """Interpreta o texto de um CSV ESTBAN (wide BCB com preâmbulo, ou genérico)."""
    skip = _sniffar_linha_header(texto)
    if skip is not None:
        return pd.read_csv(
            io.StringIO(texto), sep=";", skiprows=skip, dtype=str, low_memory=False
        )
    # fallback: formato longo / genérico (Base dos Dados, mirrors)
    for sep in (";", ","):
        try:
            df = pd.read_csv(io.StringIO(texto), sep=sep, dtype=str, low_memory=False)
        except pd.errors.ParserError:
            continue
        if df.shape[1] > 1:
            return df
    raise ValueError("Não foi possível interpretar o conteúdo como CSV ESTBAN.")


def _ler_arquivo(path: Path) -> pd.DataFrame:
    """Lê um único arquivo ESTBAN (.zip do BCB, CSV, parquet) → DataFrame raw."""
    suf = path.suffix.lower()
    if suf in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if suf == ".zip":
        with zipfile.ZipFile(path) as zf:
            nomes = [
                n for n in zf.namelist() if n.lower().endswith((".csv", ".txt"))
            ]
            if not nomes:
                raise ValueError(f"Nenhum CSV dentro de {path.name}")
            texto = zf.read(nomes[0]).decode("latin-1")
        return _ler_csv_texto(texto)
    if suf == ".gz":
        return pd.read_csv(
            path, sep=";", encoding="latin-1", dtype=str,
            compression="gzip", low_memory=False,
        )
    if suf in {".csv", ".txt"}:
        # latin-1 é o encoding do BCB e decodifica qualquer byte; utf-8-sig
        # fica como fallback explícito para arquivos de terceiros.
        try:
            texto = path.read_text(encoding="latin-1")
        except UnicodeDecodeError:
            texto = path.read_text(encoding="utf-8-sig")
        return _ler_csv_texto(texto)
    raise ValueError(f"Formato não suportado: {path.suffix}")


def _prefiltrar_uf(df: pd.DataFrame, uf: str = "CE") -> pd.DataFrame:
    """Reduz o DataFrame raw às linhas da UF alvo ANTES do melt (performance).

    Sem coluna de UF detectável, devolve o df intacto (o filtro fino por
    código IBGE acontece depois, em ``_filtrar_ce_bnb``).
    """
    for col in df.columns:
        if _slug(col) in {"uf", "sigla_uf"}:
            mask = df[col].astype(str).str.strip().str.upper() == uf
            return df[mask].copy()
    return df


def _melt_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Derrete o layout wide do BCB (verbetes em colunas) para o formato longo.

    Colunas ``VERBETE_NNN_*`` viram pares (CD_VERBETE_ESTBAN=NNN, SALDO).
    DataFrames sem colunas de verbete (já longos) passam intactos.
    """
    mapa_verbete: dict[str, str] = {}
    for col in df.columns:
        m = _RE_COL_VERBETE.match(str(col))
        if m:
            mapa_verbete[col] = m.group(1).zfill(3)
    if not mapa_verbete:
        return df

    id_vars = [c for c in df.columns if c not in mapa_verbete]
    longo = df.melt(
        id_vars=id_vars,
        value_vars=list(mapa_verbete),
        var_name="_coluna_verbete",
        value_name="SALDO",
    )
    longo["CD_VERBETE_ESTBAN"] = longo["_coluna_verbete"].map(mapa_verbete)
    return longo.drop(columns="_coluna_verbete")


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
    # fillna("") protege contra CODMUN_IBGE nulo (ocorre em arquivos reais,
    # ex.: BTG/Fortaleza em 202209) — sem isso o NaN explode nos filtros.
    out["cod_ibge"] = (
        df[canon["cod_ibge"]].astype(str).str.replace(r"\D", "", regex=True).fillna("")
    )
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

    # Ano/mês: tentar a partir de data_base (AAAAMM/AAAAMMDD), ou ano + mes diretos
    if "data_base" in canon:
        s = df[canon["data_base"]].astype(str).str.replace(r"\D", "", regex=True)
        out["ano"] = pd.to_numeric(s.str[:4], errors="coerce").astype("Int64")
        out["mes"] = pd.to_numeric(s.str[4:6], errors="coerce").astype("Int64")
    elif "ano" in canon and "mes" in canon:
        out["ano"] = pd.to_numeric(df[canon["ano"]], errors="coerce").astype("Int64")
        out["mes"] = pd.to_numeric(df[canon["mes"]], errors="coerce").astype("Int64")
    else:
        raise ValueError("Não foi possível identificar ano/mês no arquivo.")

    return out


def _filtrar_ce_bnb(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica filtros: município CE + Banco do Nordeste."""
    cods_ce = set(get_codigos_municipios_ce())
    # Códigos de 6 dígitos (sem DV) → oficial de 7 via prefixo (mesma lição do
    # CAGED: concatenar "0" só funciona para DV=0).
    mapa_prefixo6 = {c[:6]: c for c in cods_ce}

    # Filtro CE: por código IBGE (preferido) ou UF (fallback)
    df["cod_ibge"] = df["cod_ibge"].fillna("").astype(str)
    df["_ibge7"] = df["cod_ibge"].map(
        lambda x: x if isinstance(x, str) and len(x) == 7 else mapa_prefixo6.get(x, "")
    )
    mask_ce_por_codigo = df["_ibge7"].isin(cods_ce) | df["cod_ibge"].str.startswith(
        COD_IBGE_CE
    )
    if "uf" in df.columns:
        mask_ce = mask_ce_por_codigo | (df["uf"] == "CE")
    else:
        mask_ce = mask_ce_por_codigo
    df = df[mask_ce].copy()

    # Filtro BNB: por CNPJ raiz ou nome (arquivos reais usam "BCO DO NORDESTE
    # DO BRASIL S.A." — "BANCO DO NORDESTE" sozinho não casa)
    mask_bnb = pd.Series(False, index=df.index)
    if "cnpj_raiz" in df.columns:
        mask_bnb |= df["cnpj_raiz"] == ISPB_BNB_RAIZ
    if "instituicao" in df.columns:
        mask_bnb |= df["instituicao"].str.contains(
            r"B(?:ANCO|CO)\.?\s+DO\s+NORDESTE", regex=True, na=False
        )
    if not mask_bnb.any():
        logger.warning("Nenhum registro do BNB encontrado no arquivo.")
    return df[mask_bnb].copy()


def _agregar_municipal(df: pd.DataFrame) -> pd.DataFrame:
    """Pivota para (cod_ibge, ano, mes) × verbete de crédito.

    ``credito_bnb_total`` = verbete 160 puro (saldo total de operações de
    crédito). NÃO somamos 160+161+... porque 161..169 são aberturas do 160.
    """
    if df.empty:
        return pd.DataFrame()

    df = df[df["verbete"].isin(VERBETES_CREDITO)].copy()
    if df.empty:
        logger.warning("Nenhum verbete de crédito (160/161/.../169) encontrado.")
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

    col_total = f"bnb_{VERBETES_CREDITO[VERBETE_CREDITO_TOTAL]}"
    if col_total in pivot.columns:
        pivot["credito_bnb_total"] = pivot[col_total]
    else:
        # fallback (fontes longas que não trazem o agregado 160): somar aberturas
        cols_credito = [
            f"bnb_{v}"
            for k, v in VERBETES_CREDITO.items()
            if k != VERBETE_CREDITO_TOTAL and f"bnb_{v}" in pivot.columns
        ]
        logger.warning(
            f"Verbete {VERBETE_CREDITO_TOTAL} ausente; usando soma das aberturas "
            f"{cols_credito} como total."
        )
        pivot["credito_bnb_total"] = pivot[cols_credito].sum(axis=1)

    pivot = pivot.sort_values(["ano", "mes", "cod_ibge"]).reset_index(drop=True)
    return pivot


def _montar_painel(consolidado: pd.DataFrame) -> pd.DataFrame:
    """Anexa região de planejamento (SEPLAG/IPECE) e seleciona a métrica do painel.

    Saída: cod_ibge, regiao_codigo, regiao_nome, ano, mes, credito_operacoes.
    Municípios sem agência BNB simplesmente não têm linha (≠ crédito zero).
    """
    if consolidado.empty:
        return pd.DataFrame()
    info = get_regiao_info()[["cod_ibge", "regiao_codigo", "regiao_nome"]]
    painel = consolidado.merge(info, on="cod_ibge", how="inner")
    descartados = sorted(set(consolidado["cod_ibge"]) - set(painel["cod_ibge"]))
    if descartados:
        logger.warning(
            f"ESTBAN: {len(descartados)} códigos sem match no mapa de regiões CE "
            f"descartados: {descartados}"
        )
    painel = painel.rename(columns={"credito_bnb_total": "credito_operacoes"})
    painel = painel[
        ["cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes", "credito_operacoes"]
    ]
    return painel.sort_values(["ano", "mes", "cod_ibge"]).reset_index(drop=True)


def coletar_de_diretorio(diretorio: Path | str | None = None) -> pd.DataFrame:
    """
    Lê todos os arquivos ESTBAN em ``diretorio`` (default
    ``dados_nordeste/raw/estban``), aplica filtros CE+BNB e agrega por município/mês.

    Salva:
    - ``processed/estban/credito_bnb_municipio_ce_mensal.csv`` (detalhe por verbete)
    - ``processed/estban/credito_municipio_ce_mensal.csv`` (painel com regiões)

    Retorna o DataFrame do painel.
    """
    diretorio = Path(diretorio) if diretorio else (RAW_DIR / "estban")
    if not diretorio.exists():
        logger.warning(
            f"Diretório {diretorio} não existe. Rode baixar_estban_bcb() ou "
            "python3 -m pipeline.extract.estban para baixar os arquivos do BCB."
        )
        return pd.DataFrame()

    arquivos = [
        p
        for p in sorted(diretorio.rglob("*"))
        if p.is_file()
        and p.suffix.lower() in {".csv", ".txt", ".gz", ".zip", ".parquet", ".pq"}
    ]
    if not arquivos:
        logger.warning(f"Nenhum arquivo ESTBAN encontrado em {diretorio}.")
        return pd.DataFrame()

    logger.info(f"ESTBAN: lendo {len(arquivos)} arquivos de {diretorio}")
    frames = []
    for arq in arquivos:
        try:
            raw = _ler_arquivo(arq)
            raw_ce = _prefiltrar_uf(raw, "CE")
            longo = _melt_wide(raw_ce)
            norm = _normalizar(longo)
            filt = _filtrar_ce_bnb(norm)
            if not filt.empty:
                frames.append(filt)
            logger.info(f"  {arq.name}: {len(raw)} → {len(filt)} (CE+BNB, longo)")
        except Exception as e:
            logger.error(f"  {arq.name}: erro — {e}")

    if not frames:
        logger.warning("ESTBAN: nenhum dado CE+BNB extraído.")
        return pd.DataFrame()

    bruto = pd.concat(frames, ignore_index=True)
    consolidado = _agregar_municipal(bruto)
    painel = _montar_painel(consolidado)

    if not consolidado.empty:
        save_dataframe(
            consolidado,
            "credito_bnb_municipio_ce_mensal",
            subdir="processed",
            path_parts=["estban"],
        )
    if not painel.empty:
        save_dataframe(
            painel,
            "credito_municipio_ce_mensal",
            subdir="processed",
            path_parts=["estban"],
        )
        logger.info(
            f"ESTBAN: painel {len(painel)} linhas "
            f"({painel['cod_ibge'].nunique()} municípios CE × "
            f"{painel.groupby(['ano', 'mes']).ngroups} meses)"
        )
    return painel


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Coletor ESTBAN municipal CE (BNB)")
    parser.add_argument("--sem-download", action="store_true",
                        help="só processa o que já está em raw/estban")
    parser.add_argument("--ano-inicio", type=int, default=PERIODO_INICIO)
    parser.add_argument("--ano-fim", type=int, default=PERIODO_FIM)
    args = parser.parse_args()

    if not args.sem_download:
        baixar_estban_bcb(ano_inicio=args.ano_inicio, ano_fim=args.ano_fim)
    df = coletar_de_diretorio()
    if not df.empty:
        print(df.head())
        print(f"\nTotal: {len(df)} linhas, {df['cod_ibge'].nunique()} municípios")

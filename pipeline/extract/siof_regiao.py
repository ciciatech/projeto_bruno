"""
Coletor do relatório 544 do SIOF-CE — execução orçamentária por Região de
Planejamento (família "Outros"), com filtro grupo 44 (Investimentos) +
elemento 51 (Obras) ou 52 (Equipamentos).

Decisão e mecânica documentadas em ``docs/decisao-sci01-siof-regional.md``:
o SIOF-Web (WebForms ASP.NET, sem ``__EVENTVALIDATION``) exige um postback de
troca de família (``__EVENTTARGET=ctl00$cphCorpo$rblRelatorio$3`` →
``rblRelatorio="Outros"``) antes do POST de ``btnVisualizar`` com
``ddlRelatorio=544``; o XLS sai em ``/siofconsulta/Exports/rel_*.XLS``.
Módulo independente do ``pipeline/extract/siof.py`` (família "Secretaria"),
que permanece intocado.

Cadência: BIMESTRAL-ACUMULADA — o relatório aceita ``mês`` para qualquer ano e
devolve valores acumulados até o mês; coletamos mês ∈ {2,4,6,8,10,12}
(mês=12 = fechamento anual). O fluxo bimestral é a diferença do acumulado
entre bimestres consecutivos (bimestre 1 = acumulado de fevereiro).

Esquema regional dual:
- 2016+ — 14 regiões de planejamento (LC 154/2015), códigos/nomes idênticos a
  ``pipeline.regioes_ce.REGIOES_CE``; código "15" = "Estado do Ceará"
  (despesa não regionalizada — mantida com flag no nome, sem rateio).
- 2015 — 8 macrorregiões antigas; salvas com ``regiao_codigo`` prefixado "A"
  (A1–A8 + A22 "Estado do Ceará") e nomes originais do relatório. NÃO se
  misturam com as 14 regiões (crosswalk 14→8 é a task T47).

Cache bruto por request em ``dados_nordeste/raw/siof_regiao/<ano>_<mes>_<elemento>.xls``
(retomável: pulamos o download se o arquivo existir). Throttle ≥1,5s entre
requests ao SIOF (sistema governamental).

Validação: o fechamento anual (mês=12) deve bater célula a célula
(rtol 1e-6) com o gabarito do spike
``docs/sci01-spike/siof_544_consolidado_2015_2025.csv``.

Uso: ``python3 -m pipeline.extract.siof_regiao`` (da raiz do repo).
"""

from __future__ import annotations

import io
import re
import time
import logging
import warnings
from pathlib import Path

import numpy as np
import requests
import pandas as pd

from pipeline.config import RAW_DIR, MAX_RETRIES, RETRY_DELAY
from pipeline.utils import save_dataframe, setup_logging
from pipeline.regioes_ce import REGIOES_CE

# Suprimir warnings de SSL para o servidor do SIOF (certificado com cadeia incompleta)
warnings.filterwarnings(
    "ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning
)

logger = logging.getLogger(__name__)

# ==============================================================================
# Constantes do relatório 544
# ==============================================================================

BASE_URL = "https://planejamento.seplag.ce.gov.br/siofconsulta/Paginas/frm_consulta_execucao.aspx"
EXPORTS_URL = "https://planejamento.seplag.ce.gov.br/siofconsulta/Exports/"

RELATORIO_REGIAO = "544"          # família "Outros" — Região × Lei/Lei+Créd/Empenhado/Pago
FAMILIA_OUTROS_INDICE = 3         # ctl00$cphCorpo$rblRelatorio$3 → "Outros"
GRUPO_INVESTIMENTOS = "44"

# label → valor do dropdown ddlDespElemento (os labels casam com o spike)
ELEMENTOS = {
    "obras": "201290",   # 51 - Obras e Instalações
    "equip": "137",      # 52 - Equipamentos e Material Permanente
}
# label → código orçamentário esperado no rodapé "Critérios:" do XLS
ELEMENTO_COD_RODAPE = {"obras": "51", "equip": "52"}

MESES_BIMESTRAIS = (2, 4, 6, 8, 10, 12)
ANOS_PADRAO = tuple(range(2015, 2026))     # 2015-2025
THROTTLE_SEGUNDOS = 1.5

RAW_SIOF_REGIAO_DIR = RAW_DIR / "siof_regiao"
SPIKE_CSV = Path("docs") / "sci01-spike" / "siof_544_consolidado_2015_2025.csv"

FLAG_NAO_REGIONALIZADO = "não regionalizado"
ULTIMO_ANO_ESQUEMA_ANTIGO = 2015  # ≤2015: 8 macrorregiões antigas (códigos A*)

# ==============================================================================
# Throttle global (≥1,5s entre quaisquer requests ao SIOF)
# ==============================================================================

_ultimo_request_ts = 0.0


def _throttle():
    """Garante intervalo mínimo de THROTTLE_SEGUNDOS desde o último request ao SIOF."""
    global _ultimo_request_ts
    espera = _ultimo_request_ts + THROTTLE_SEGUNDOS - time.monotonic()
    if espera > 0:
        time.sleep(espera)
    _ultimo_request_ts = time.monotonic()


# ==============================================================================
# Coletor (rede)
# ==============================================================================


class SiofRegiao544:
    """Coletor do relatório 544 no SIOF-Web (WebForms, sem ``__EVENTVALIDATION``).

    Cada relatório usa uma sessão fresca com a sequência de postbacks que
    reproduz a interação real do usuário (provada em rede para qualquer mês):

    1. GET da página (cookie + ``__VIEWSTATE``);
    2. postback de troca de família (``rblRelatorio$3`` → "Outros"), já com
       ano/mês alvo e SEM grupo/elemento;
    3. postback da cascata de grupo (``__EVENTTARGET=ddlDespGrupo``, grupo=44)
       — necessário porque o ``ddlDespElemento`` é dropdown dependente: sem
       este passo o filtro de elemento é DESCARTADO silenciosamente pelo
       servidor em meses ≠ 12 (verificado empiricamente; o rodapé "Critérios:"
       do XLS é a prova de aplicação do filtro);
    4. postback de ``btnVisualizar`` (grupo=44 + elemento) → ``window.open``
       com o nome do arquivo em ``Exports/rel_*.XLS``;
    5. GET do XLS.

    Todos os requests passam pelo throttle global (≥1,5s).
    """

    def __init__(self):
        self.n_requests = 0

    # -------------------------------------------------------------- helpers

    @staticmethod
    def _extrair_viewstate(html: str) -> str:
        m = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', html)
        if not m:
            raise RuntimeError("__VIEWSTATE ausente na resposta")
        return m.group(1)

    @staticmethod
    def _form_base(
        viewstate: str,
        ano: int,
        mes: int,
        grupo: str = "",
        elemento: str = "",
        incluir_relatorio: bool = True,
    ) -> dict:
        form = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__LASTFOCUS": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": "AA2FB94C",
            "ctl00$cphCorpo$ddlAno": str(ano),
            "ctl00$cphCorpo$ddlMes": f"{mes:02d}",
            "ctl00$cphCorpo$ddlSecretaria": "",
            "ctl00$cphCorpo$ddlOrgao": "",
            "ctl00$cphCorpo$ddlUnidOrcamentaria": "",
            "ctl00$cphCorpo$ddlFuncao": "",
            "ctl00$cphCorpo$ddlSubFuncao": "",
            "ctl00$cphCorpo$ddlPrograma": "",
            "ctl00$cphCorpo$ddlProjetoAtividade": "",
            "ctl00$cphCorpo$ddlRegiao": "",
            "ctl00$cphCorpo$ddlLancContabil": "",
            "ctl00$cphCorpo$ddlFonte": "",
            "ctl00$cphCorpo$ddlSubfonte": "",
            "ctl00$cphCorpo$ddlGrupofonterecurso": "",
            "ctl00$cphCorpo$ddlClassificacao": "",
            "ctl00$cphCorpo$ddlDespCategoria": "",
            "ctl00$cphCorpo$ddlDespGrupo": grupo,
            "ctl00$cphCorpo$ddlDespModalidade": "",
            "ctl00$cphCorpo$ddlDespElemento": elemento,
            "ctl00$cphCorpo$ddlGrupoFonte": "",
            "ctl00$cphCorpo$ddlGrupoPrograma": "",
            "ctl00$cphCorpo$ddlEixo": "",
            "ctl00$cphCorpo$ddlArea": "",
            "ctl00$cphCorpo$ddlPoder": "",
            "ctl00$cphCorpo$ddlIdResultadoPrimario": "",
            "ctl00$cphCorpo$ddlModalidade91": "TUDO",
            "ctl00$cphCorpo$ddlEmenda": "TUDO",
            "ctl00$cphCorpo$ddlPrevidencia": "TUDO",
            "ctl00$cphCorpo$txtCodDotacao": "",
            "ctl00$cphCorpo$rblRelatorio": "Outros",
            "ctl00$cphCorpo$rblFormato": "Xlss",
        }
        if incluir_relatorio:
            form["ctl00$cphCorpo$ddlRelatorio"] = RELATORIO_REGIAO
        return form

    def _get(self, session: requests.Session, url: str) -> requests.Response:
        _throttle()
        self.n_requests += 1
        resp = session.get(url, verify=False, timeout=120)
        resp.raise_for_status()
        return resp

    def _post(self, session: requests.Session, data: dict) -> requests.Response:
        _throttle()
        self.n_requests += 1
        resp = session.post(BASE_URL, data=data, verify=False, timeout=120)
        resp.raise_for_status()
        return resp

    # --------------------------------------------------------------- coleta

    def _baixar_uma_vez(self, ano: int, mes: int, elemento_valor: str) -> bytes:
        session = requests.Session()
        session.headers.update(
            {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        )

        # 1. GET inicial
        vs = self._extrair_viewstate(self._get(session, BASE_URL).text)

        # 2. troca de família → "Outros" (ddlRelatorio ainda é da família anterior)
        form = self._form_base(vs, ano, mes, incluir_relatorio=False)
        form["__EVENTTARGET"] = f"ctl00$cphCorpo$rblRelatorio${FAMILIA_OUTROS_INDICE}"
        vs = self._extrair_viewstate(self._post(session, form).text)

        # 3. cascata do grupo de despesa (popula ddlDespElemento p/ grupo 44)
        form = self._form_base(vs, ano, mes, grupo=GRUPO_INVESTIMENTOS)
        form["__EVENTTARGET"] = "ctl00$cphCorpo$ddlDespGrupo"
        vs = self._extrair_viewstate(self._post(session, form).text)

        # 4. visualizar (gera o XLS no servidor)
        form = self._form_base(
            vs, ano, mes, grupo=GRUPO_INVESTIMENTOS, elemento=elemento_valor
        )
        form["__EVENTTARGET"] = "ctl00$cphCorpo$btnVisualizar"
        resp = self._post(session, form)
        m = re.search(
            r"window\.open\(['\"]\.\./Exports/(rel_[^'\"]+)['\"]", resp.text
        )
        if not m:
            alerts = re.findall(r"alert\(['\"]([^'\"]+)['\"]\)", resp.text)
            raise RuntimeError(f"URL do XLS não encontrada (alerts={alerts[:3]})")

        # 5. download
        blob = self._get(session, EXPORTS_URL + m.group(1)).content
        if not blob.startswith(b"\xd0\xcf\x11\xe0"):
            raise RuntimeError("download não é um XLS (OLE2) válido")
        return blob

    def baixar_xls(self, ano: int, mes: int, elemento: str) -> bytes:
        """Gera e baixa o XLS do 544 para (ano, mês, elemento) com retries.

        ``elemento`` é o label ("obras" | "equip"). Lança RuntimeError após
        MAX_RETRIES tentativas (cada tentativa usa sessão fresca).
        """
        ultima_falha: Exception | None = None
        for tentativa in range(1, MAX_RETRIES + 1):
            try:
                return self._baixar_uma_vez(ano, mes, ELEMENTOS[elemento])
            except Exception as e:  # RequestException, 504, viewstate, etc.
                ultima_falha = e
                logger.warning(
                    f"SIOF-Região 544 {ano}/{mes:02d} {elemento}: tentativa "
                    f"{tentativa}/{MAX_RETRIES} falhou — {e}"
                )
                time.sleep(RETRY_DELAY * tentativa)

        raise RuntimeError(
            f"SIOF-Região 544 {ano}/{mes:02d} {elemento}: falha definitiva após "
            f"{MAX_RETRIES} tentativas — {ultima_falha}"
        )

    def obter_xls(self, ano: int, mes: int, elemento: str) -> Path:
        """Devolve o path do XLS bruto, usando cache (retomável) ou baixando."""
        cache = RAW_SIOF_REGIAO_DIR / f"{ano}_{mes:02d}_{elemento}.xls"
        if cache.exists() and cache.stat().st_size > 0:
            logger.debug(f"SIOF-Região: cache — {cache.name}")
            return cache
        logger.info(f"SIOF-Região 544: baixando {ano}/{mes:02d} {elemento}...")
        blob = self.baixar_xls(ano, mes, elemento)
        RAW_SIOF_REGIAO_DIR.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(blob)
        return cache


# ==============================================================================
# Parser (puro — testável sem rede)
# ==============================================================================


def _normalizar_codigo(valor) -> str | None:
    """'01'/'22'/1.0/'Total Geral' → '01'/'22'/'01'/None (None = não é região)."""
    s = str(valor).strip()
    if re.fullmatch(r"\d+(\.0)?", s):
        return s.split(".")[0].zfill(2)
    return None


def _aplicar_esquema(cod: str, descricao: str, ano: int) -> tuple[str, str]:
    """Aplica o esquema regional do ano: (regiao_codigo, regiao_nome).

    - ano ≤ 2015 (8 macrorregiões antigas): código prefixado "A" sem zero à
      esquerda (A1-A8, A22) e nome original do relatório.
    - ano ≥ 2016 (14 regiões LC 154/2015): códigos "01"-"14" com nomes
      EXATAMENTE como ``REGIOES_CE``; "15" = Estado do Ceará.
    - Linha não-regionalizada ("Estado do Ceará", cód. 15/22): mantida com
      flag clara no nome — quem consome decide; NÃO rateamos.
    """
    descricao = str(descricao).strip()
    if ano <= ULTIMO_ANO_ESQUEMA_ANTIGO:
        codigo = f"A{int(cod)}"
        nome = descricao
        if "CEAR" in descricao.upper():
            nome = f"{descricao} ({FLAG_NAO_REGIONALIZADO})"
        return codigo, nome

    if cod in REGIOES_CE:
        return cod, REGIOES_CE[cod]
    if cod == "15":
        return "15", f"Estado do Ceará ({FLAG_NAO_REGIONALIZADO})"
    logger.warning(f"SIOF-Região: código de região inesperado em {ano}: {cod} ({descricao})")
    return cod, descricao


def parse_xls_544(content: bytes, ano: int, mes: int, elemento: str) -> pd.DataFrame:
    """Converte o XLS do relatório 544 em DataFrame tidy.

    Colunas: ano, mes, bimestre, elemento, regiao_codigo, regiao_nome,
    empenhado_acum, pago_acum. Valida no rodapé "Critérios:" que os filtros
    grupo 44 + elemento 51/52 foram de fato aplicados (o rel. 120 NÃO aplica).
    """
    df_raw = pd.read_excel(io.BytesIO(content), header=None)
    if df_raw.empty:
        raise ValueError(f"XLS vazio — 544 {ano}/{mes:02d} {elemento}")

    texto_completo = " ".join(str(v) for v in df_raw.values.ravel() if pd.notna(v))
    cod_esperado = ELEMENTO_COD_RODAPE[elemento]
    criterio = f"Elemento da Despesa: {cod_esperado}"
    if criterio not in texto_completo or "Grupo da Despesa: 44" not in texto_completo:
        raise ValueError(
            f"Rodapé 'Critérios:' não confirma grupo 44 + elemento {cod_esperado} "
            f"— 544 {ano}/{mes:02d} {elemento}"
        )

    # localizar cabeçalho ("Código" | "Descrição" | Lei | Lei+Cred | Empenhado | Pago ...)
    header_row = None
    for i, row in df_raw.iterrows():
        valores = [str(v).strip().lower() for v in row.values]
        if "código" in valores or "codigo" in valores:
            header_row = i
            break
    if header_row is None:
        raise ValueError(f"Cabeçalho não encontrado — 544 {ano}/{mes:02d} {elemento}")

    colunas = [str(c).strip() for c in df_raw.iloc[header_row].values]
    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = colunas

    col_emp = next(c for c in df.columns if c.lower().startswith("empenhado"))
    col_pago = next(c for c in df.columns if c.lower() == "pago")
    col_cod = next(c for c in df.columns if "digo" in c.lower())
    col_desc = next(c for c in df.columns if "descri" in c.lower())

    linhas = []
    for _, row in df.iterrows():
        cod = _normalizar_codigo(row[col_cod])
        if cod is None:  # Total Geral / Critérios / rodapé
            continue
        codigo, nome = _aplicar_esquema(cod, row[col_desc], ano)
        emp = pd.to_numeric(row[col_emp], errors="coerce")
        pago = pd.to_numeric(row[col_pago], errors="coerce")
        linhas.append(
            {
                "ano": ano,
                "mes": mes,
                "bimestre": mes // 2,
                "elemento": elemento,
                "regiao_codigo": codigo,
                "regiao_nome": nome,
                "empenhado_acum": 0.0 if pd.isna(emp) else float(emp),
                "pago_acum": 0.0 if pd.isna(pago) else float(pago),
            }
        )

    if not linhas:
        raise ValueError(f"Nenhuma linha de região — 544 {ano}/{mes:02d} {elemento}")
    return pd.DataFrame(linhas)


# ==============================================================================
# Fluxo bimestral (diff do acumulado) + sanidade
# ==============================================================================


def calcular_fluxos(df_acum: pd.DataFrame) -> pd.DataFrame:
    """Fluxo bimestral = diff do acumulado entre bimestres consecutivos.

    Bimestre 1 (acumulado de fevereiro) não tem anterior: fluxo = acumulado.
    Se uma região só aparece a partir de um bimestre, o primeiro fluxo dela é
    o próprio acumulado.
    """
    df = df_acum.sort_values(
        ["ano", "elemento", "regiao_codigo", "bimestre"]
    ).reset_index(drop=True)
    grupo = df.groupby(["ano", "elemento", "regiao_codigo"], sort=False)
    for est in ("empenhado", "pago"):
        fluxo = grupo[f"{est}_acum"].diff()
        df[f"{est}_fluxo"] = fluxo.fillna(df[f"{est}_acum"]).round(2)
    return df


def checar_sanidade_acumulados(df: pd.DataFrame) -> pd.DataFrame:
    """Acumulado deve ser não-decrescente ao longo dos bimestres do ano.

    Fluxos negativos (anulações de empenho/estornos) são LOGADOS como warning
    e devolvidos para inspeção — não derrubam a coleta.
    """
    mask = (df["empenhado_fluxo"] < 0) | (df["pago_fluxo"] < 0)
    # bimestre 1 nunca é negativo por construção (fluxo = acumulado ≥ 0)
    negativos = df[mask]
    if len(negativos) > 0:
        pior = min(negativos["empenhado_fluxo"].min(), negativos["pago_fluxo"].min())
        logger.warning(
            f"SIOF-Região: {len(negativos)} fluxo(s) bimestral(is) negativo(s) "
            f"(anulações/estornos; pior = {pior:,.2f}). Acumulado não é "
            "estritamente não-decrescente nessas células — mantidas no CSV."
        )
        for _, r in negativos.iterrows():
            logger.warning(
                f"  fluxo negativo: {r['ano']} b{r['bimestre']} {r['elemento']} "
                f"{r['regiao_codigo']} {r['regiao_nome']}: "
                f"emp={r['empenhado_fluxo']:,.2f} pago={r['pago_fluxo']:,.2f}"
            )
    return negativos


# ==============================================================================
# Validação contra o gabarito do spike (mês=12)
# ==============================================================================


def validar_contra_spike(
    df_anual: pd.DataFrame,
    spike_csv: Path = SPIKE_CSV,
    rtol: float = 1e-6,
) -> pd.DataFrame:
    """Compara o fechamento anual com o consolidado do spike, célula a célula.

    Devolve DataFrame de divergências (vazio = OK). Também loga cobertura:
    TODAS as células comuns devem bater; células do spike sem par na coleta
    são reportadas como divergência de cobertura.
    """
    spike = pd.read_csv(spike_csv, dtype={"cod": str})
    spike["cod"] = spike["cod"].str.strip().str.zfill(2)

    base = df_anual.copy()
    # "A1"/"A22" (2015) e "01".."15" (2016+) → código bruto do relatório ("01".."22")
    base["cod"] = (
        base["regiao_codigo"].astype(str).str.replace("A", "", regex=False).str.zfill(2)
    )
    base = base[["ano", "elemento", "cod", "empenhado_acum", "pago_acum"]]

    merged = spike.merge(base, on=["ano", "elemento", "cod"], how="left", indicator=True)

    problemas = []
    sem_par = merged[merged["_merge"] == "left_only"]
    for _, r in sem_par.iterrows():
        problemas.append(
            {"ano": r["ano"], "elemento": r["elemento"], "cod": r["cod"],
             "campo": "cobertura", "spike": np.nan, "coleta": np.nan,
             "detalhe": "célula do spike ausente na coleta"}
        )

    ambos = merged[merged["_merge"] == "both"]
    for campo, col_spike, col_base in (
        ("empenhado", "empenhado", "empenhado_acum"),
        ("pago", "pago", "pago_acum"),
    ):
        diverge = ~np.isclose(
            ambos[col_spike].astype(float), ambos[col_base].astype(float),
            rtol=rtol, atol=0.0,
        )
        for _, r in ambos[diverge].iterrows():
            problemas.append(
                {"ano": r["ano"], "elemento": r["elemento"], "cod": r["cod"],
                 "campo": campo, "spike": r[col_spike], "coleta": r[col_base],
                 "detalhe": "valor divergente"}
            )

    divergencias = pd.DataFrame(
        problemas,
        columns=["ano", "elemento", "cod", "campo", "spike", "coleta", "detalhe"],
    )
    n_comuns = len(ambos)
    logger.info(
        f"SIOF-Região: validação vs spike — {n_comuns}/{len(spike)} células do "
        f"gabarito casadas; {len(divergencias)} divergência(s)."
    )
    return divergencias


# ==============================================================================
# Orquestração
# ==============================================================================


def coletar_tudo(
    anos=ANOS_PADRAO,
    meses=MESES_BIMESTRAIS,
    elementos=tuple(ELEMENTOS),
) -> dict:
    """Coleta 544 para anos×meses×elementos, gera CSVs processados e valida.

    Retorna dict de métricas. Lança RuntimeError se o fechamento anual
    divergir do gabarito do spike.
    """
    coletor = SiofRegiao544()
    frames, falhas = [], []
    n_alvos = len(anos) * len(meses) * len(elementos)

    for ano in anos:
        for mes in meses:
            for elemento in elementos:
                try:
                    path = coletor.obter_xls(ano, mes, elemento)
                    frames.append(parse_xls_544(path.read_bytes(), ano, mes, elemento))
                except Exception as e:
                    falhas.append((ano, mes, elemento, str(e)))
                    logger.error(f"SIOF-Região: pulando {ano}/{mes:02d} {elemento} — {e}")

    if not frames:
        raise RuntimeError("SIOF-Região: nenhum relatório coletado.")

    df_bim = calcular_fluxos(pd.concat(frames, ignore_index=True))
    negativos = checar_sanidade_acumulados(df_bim)

    cols = [
        "ano", "bimestre", "regiao_codigo", "regiao_nome", "elemento",
        "empenhado_acum", "pago_acum", "empenhado_fluxo", "pago_fluxo",
    ]
    df_bim_out = df_bim[cols].copy()
    save_dataframe(
        df_bim_out, "siof_regiao_bimestral",
        subdir="processed", path_parts=["execucao_orcamentaria", "ce"],
    )

    df_anual = df_bim[df_bim["mes"] == 12]
    df_anual_out = df_anual[cols].copy()
    save_dataframe(
        df_anual_out, "siof_regiao_anual",
        subdir="processed", path_parts=["execucao_orcamentaria", "ce"],
    )

    divergencias = pd.DataFrame()
    if SPIKE_CSV.exists():
        divergencias = validar_contra_spike(df_anual)
        if not divergencias.empty:
            logger.error(
                "SIOF-Região: fechamento anual DIVERGE do gabarito do spike:\n"
                + divergencias.to_string(index=False)
            )
            raise RuntimeError(
                f"SIOF-Região: {len(divergencias)} divergência(s) vs spike — "
                "investigar antes de prosseguir."
            )
    else:
        logger.warning(f"SIOF-Região: gabarito do spike não encontrado em {SPIKE_CSV}")

    metricas = {
        "alvos": n_alvos,
        "coletados": n_alvos - len(falhas),
        "falhas": falhas,
        "requests_http": coletor.n_requests,
        "linhas_bimestral": len(df_bim_out),
        "linhas_anual": len(df_anual_out),
        "anos": sorted(df_bim["ano"].unique().tolist()),
        "bimestres_por_ano": df_bim.groupby("ano")["bimestre"].nunique().to_dict(),
        "fluxos_negativos": len(negativos),
        "divergencias_spike": len(divergencias),
    }
    return metricas


if __name__ == "__main__":
    setup_logging()
    inicio = time.monotonic()
    met = coletar_tudo()
    dur = time.monotonic() - inicio
    logger.info("=" * 70)
    logger.info(f"SIOF-Região 544 — coleta concluída em {dur/60:.1f} min")
    logger.info(f"  relatórios: {met['coletados']}/{met['alvos']} (falhas: {len(met['falhas'])})")
    logger.info(f"  requests HTTP ao SIOF: {met['requests_http']}")
    logger.info(f"  linhas bimestral: {met['linhas_bimestral']} | anual: {met['linhas_anual']}")
    logger.info(f"  anos: {met['anos']}")
    logger.info(f"  bimestres por ano: {met['bimestres_por_ano']}")
    logger.info(f"  fluxos negativos (anulações): {met['fluxos_negativos']}")
    logger.info(f"  divergências vs spike: {met['divergencias_spike']}")
    if met["falhas"]:
        for f in met["falhas"]:
            logger.error(f"  FALHA: {f}")

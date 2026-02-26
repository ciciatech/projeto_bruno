"""
Extração de dados de execução orçamentária do SIOF-CE (SEPLAG/CE).

O SIOF não possui API REST — usa WebForms ASP.NET com ViewState.
Fluxo: GET (captura sessão + __VIEWSTATE) → POST (gera relatório) → GET (baixa XLS).
"""

import io
import re
import time
import logging
import warnings

import requests
import pandas as pd

from pipeline.config import RAW_DIR, PERIODO_INICIO, PERIODO_FIM, MAX_RETRIES, RETRY_DELAY
from pipeline.utils import save_dataframe

# Suprimir warnings de SSL para o servidor do SIOF
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class SiofCE:
    """
    Coleta de dados de execução orçamentária do SIOF-CE (SEPLAG/CE).

    Fonte: https://planejamento.seplag.ce.gov.br/siofconsulta/
    """

    BASE_URL = "https://planejamento.seplag.ce.gov.br/siofconsulta/Paginas/frm_consulta_execucao.aspx"
    EXPORTS_URL = "https://planejamento.seplag.ce.gov.br/siofconsulta/Exports/"

    # Relatórios disponíveis (subset relevante para a tese)
    RELATORIOS = {
        "101": "secretaria",
        "102": "secretaria_fonte",
        "105": "secretaria_orgao_classificacao",
    }

    @staticmethod
    def coletar_relatorio(ano: int, mes: int, relatorio: str = "101") -> pd.DataFrame:
        """Coleta um relatório específico do SIOF-CE para ano/mês."""
        cache_path = RAW_DIR / f"siof_{ano}_{mes}_{relatorio}.parquet"
        if cache_path.exists():
            logger.info(f"SIOF-CE: Cache encontrado {cache_path.name}, pulando download.")
            return pd.read_parquet(cache_path)

        nome_rel = SiofCE.RELATORIOS.get(relatorio, relatorio)
        logger.info(f"SIOF-CE: Coletando relatório {relatorio} ({nome_rel}) — {ano}/{mes:02d}...")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                session = requests.Session()

                # 1. GET — captura __VIEWSTATE e cookie de sessão
                resp_get = session.get(SiofCE.BASE_URL, verify=False, timeout=60)
                resp_get.raise_for_status()

                match_vs = re.search(
                    r'id="__VIEWSTATE"\s+value="([^"]*)"', resp_get.text
                )
                if not match_vs:
                    logger.warning(
                        f"SIOF-CE: __VIEWSTATE não encontrado (tentativa {attempt}/{MAX_RETRIES})"
                    )
                    time.sleep(RETRY_DELAY * attempt)
                    continue

                viewstate = match_vs.group(1)

                # 2. POST — solicita geração do relatório
                form_data = {
                    "__EVENTTARGET": "ctl00$cphCorpo$btnVisualizar",
                    "__EVENTARGUMENT": "",
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": viewstate,
                    "__VIEWSTATEGENERATOR": "AA2FB94C",
                    "ctl00$cphCorpo$ddlAno": str(ano),
                    "ctl00$cphCorpo$ddlMes": str(mes),
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
                    "ctl00$cphCorpo$ddlDespGrupo": "",
                    "ctl00$cphCorpo$ddlDespModalidade": "",
                    "ctl00$cphCorpo$ddlDespElemento": "",
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
                    "ctl00$cphCorpo$rblRelatorio": "Secretaria",
                    "ctl00$cphCorpo$ddlRelatorio": relatorio,
                    "ctl00$cphCorpo$rblFormato": "Xlss",
                }

                resp_post = session.post(
                    SiofCE.BASE_URL, data=form_data, verify=False, timeout=90
                )
                resp_post.raise_for_status()

                # 3. Extrair URL do arquivo gerado
                match_file = re.search(
                    r"window\.open\(['\"]\.\.\/Exports\/(rel_[^'\"]+)['\"]",
                    resp_post.text,
                )
                if not match_file:
                    logger.warning(
                        f"SIOF-CE: URL do arquivo não encontrada na resposta "
                        f"(tentativa {attempt}/{MAX_RETRIES})"
                    )
                    time.sleep(RETRY_DELAY * attempt)
                    continue

                filename = match_file.group(1)
                file_url = SiofCE.EXPORTS_URL + filename

                # 4. GET — download do XLS
                resp_file = session.get(file_url, verify=False, timeout=120)
                resp_file.raise_for_status()

                # 5. Parse do XLS
                df = SiofCE._parse_xls(resp_file.content, ano, mes)
                if df.empty:
                    logger.warning(f"SIOF-CE: Relatório {relatorio} {ano}/{mes:02d} retornou vazio.")
                    return df

                save_dataframe(df, f"siof_{relatorio}_{ano}_{mes}")
                return df

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"SIOF-CE: Erro na requisição (tentativa {attempt}/{MAX_RETRIES}): {e}"
                )
                time.sleep(RETRY_DELAY * attempt)

        logger.error(
            f"SIOF-CE: Falha definitiva após {MAX_RETRIES} tentativas — "
            f"relatório {relatorio} {ano}/{mes:02d}"
        )
        return pd.DataFrame()

    @staticmethod
    def _parse_xls(content: bytes, ano: int, mes: int) -> pd.DataFrame:
        """Lê XLS bruto, detecta cabeçalho, limpa e normaliza."""
        df_raw = pd.read_excel(io.BytesIO(content), header=None)
        if df_raw.empty:
            return pd.DataFrame()

        # Detectar linha do cabeçalho real (contém "Descrição" ou "Código")
        header_row = None
        for i, row in df_raw.iterrows():
            row_str = row.astype(str).str.lower()
            if row_str.str.contains("descrição|codigo|código").any():
                header_row = i
                break

        if header_row is None:
            logger.warning("SIOF-CE: Cabeçalho não encontrado no XLS.")
            return pd.DataFrame()

        # Recriar DataFrame com cabeçalho correto
        df = df_raw.iloc[header_row + 1:].copy()
        df.columns = df_raw.iloc[header_row].values
        df.reset_index(drop=True, inplace=True)

        # Normalizar nomes de colunas
        df.columns = [str(c).strip() for c in df.columns]

        # Identificar coluna de código (pode ser "Código" ou variação)
        col_codigo = None
        for c in df.columns:
            if "digo" in c.lower():
                col_codigo = c
                break

        # Remover linhas de totais
        if col_codigo is not None:
            mask_total = df[col_codigo].isna() | df[col_codigo].astype(str).str.upper().str.contains("TOTAL", na=False)
            df = df[~mask_total].copy()

        # Converter colunas numéricas (XLS do SIOF usa formato padrão com ponto decimal)
        # Preservar colunas de código e descrição como string
        cols_texto = set()
        if col_codigo is not None:
            cols_texto.add(col_codigo)
        for c in df.columns:
            if "descri" in c.lower():
                cols_texto.add(c)

        for col in df.columns:
            if col in cols_texto:
                continue
            if df[col].dtype == object:
                converted = pd.to_numeric(df[col], errors="coerce")
                # Se >50% dos valores não-nulos converteram, é coluna numérica
                if converted.notna().sum() > 0.5 * df[col].notna().sum():
                    df[col] = converted

        df["ano"] = ano
        df["mes"] = mes
        df.reset_index(drop=True, inplace=True)
        return df

    @classmethod
    def coletar_todas(cls) -> pd.DataFrame:
        """Coleta acumulado dezembro (2015-2025) e mês mais recente para 2026."""
        frames = []

        for ano in range(PERIODO_INICIO, PERIODO_FIM + 1):
            # Acumulado anual = dezembro
            df = cls.coletar_relatorio(ano, mes=12, relatorio="101")
            if not df.empty:
                frames.append(df)
            time.sleep(2)

        # 2026: tenta meses de dezembro até janeiro para pegar o mais recente
        for mes in range(12, 0, -1):
            df = cls.coletar_relatorio(2026, mes=mes, relatorio="101")
            if not df.empty:
                frames.append(df)
                logger.info(f"SIOF-CE: 2026 — dados disponíveis até mês {mes}.")
                break
            time.sleep(2)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siof_consolidado")
        return df_all

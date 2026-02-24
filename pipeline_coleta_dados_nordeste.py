"""
Pipeline de Coleta de Dados Públicos - Tese DESP/UFC
=====================================================
Impactos do Crédito no Crescimento Econômico do Nordeste

Fontes:
  1. BACEN-SGS: Séries de crédito PF/PJ e IBCR-NE
  2. Portal da Transparência: Transferências federais (Bolsa Família, FPE, FPM, FUNDEB)
  3. SICONFI/STN: Dados fiscais dos entes federativos do Nordeste
  4. Dados.gov.br / Vis Data MDS: Programas sociais (Bolsa Família / Auxílio Brasil)

Período: 2015-2025 (trimestral)
Região: Nordeste (9 estados)

Autor: Bruno Cardoso Costa
Orientador: Prof. Dr. Magno Prudêncio de Almeida Filho

Uso:
  pip install requests pandas tqdm openpyxl
  python pipeline_coleta_dados_nordeste.py
"""

import os
import time
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
import pandas as pd
from tqdm import tqdm

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

BASE_DIR = Path("./dados_nordeste")
RAW_DIR = BASE_DIR / "raw"
PROCESSED_DIR = BASE_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"

for d in [RAW_DIR, PROCESSED_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"coleta_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Estados do Nordeste
ESTADOS_NE = {
    "AL": {"nome": "Alagoas", "cod_ibge": "27", "cod_siafi": "27"},
    "BA": {"nome": "Bahia", "cod_ibge": "29", "cod_siafi": "29"},
    "CE": {"nome": "Ceará", "cod_ibge": "23", "cod_siafi": "23"},
    "MA": {"nome": "Maranhão", "cod_ibge": "21", "cod_siafi": "21"},
    "PB": {"nome": "Paraíba", "cod_ibge": "25", "cod_siafi": "25"},
    "PE": {"nome": "Pernambuco", "cod_ibge": "26", "cod_siafi": "26"},
    "PI": {"nome": "Piauí", "cod_ibge": "22", "cod_siafi": "22"},
    "RN": {"nome": "Rio Grande do Norte", "cod_ibge": "24", "cod_siafi": "24"},
    "SE": {"nome": "Sergipe", "cod_ibge": "28", "cod_siafi": "28"},
}

PERIODO_INICIO = 2015
PERIODO_FIM = 2025

# Timeout e retry padrão
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos


# ==============================================================================
# UTILITÁRIOS
# ==============================================================================

def safe_request(url: str, params: dict = None, headers: dict = None,
                 timeout: int = REQUEST_TIMEOUT, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """Requisição HTTP com retry e tratamento de erro."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP {resp.status_code} em {url} (tentativa {attempt}/{retries}): {e}")
            if resp.status_code == 429:
                wait = RETRY_DELAY * attempt * 2
                logger.info(f"Rate limit. Aguardando {wait}s...")
                time.sleep(wait)
            elif resp.status_code >= 500:
                time.sleep(RETRY_DELAY * attempt)
            else:
                return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Erro em {url} (tentativa {attempt}/{retries}): {e}")
            time.sleep(RETRY_DELAY * attempt)
    logger.error(f"Falha definitiva após {retries} tentativas: {url}")
    return None


def save_dataframe(df: pd.DataFrame, filename: str, subdir: str = "raw"):
    """Salva DataFrame como CSV e Parquet."""
    target_dir = RAW_DIR if subdir == "raw" else PROCESSED_DIR
    csv_path = target_dir / f"{filename}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"Salvo: {csv_path} ({len(df)} registros)")
    try:
        parquet_path = target_dir / f"{filename}.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Salvo: {parquet_path}")
    except Exception:
        logger.warning(f"Parquet não disponível para {filename}. Apenas CSV salvo.")


# ==============================================================================
# 1. BACEN - SISTEMA GERENCIADOR DE SÉRIES (SGS)
# ==============================================================================

class BacenSGS:
    """Coleta de séries temporais do Banco Central via API SGS."""

    BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"

    # Séries relevantes para a tese
    SERIES = {
        # IBCR Nordeste (com ajuste sazonal)
        25389: "IBCR_NE_ajuste_sazonal",
        # Saldo de crédito PF - Nordeste
        14084: "credito_PF_nordeste",
        # Saldo de crédito PJ - Nordeste
        14089: "credito_PJ_nordeste",
        # Saldo total de crédito - Nordeste
        14079: "credito_total_nordeste",
        # SELIC acumulada no mês (anualizada) - série mensal
        4189: "selic_mensal",
        # IPCA mensal
        433: "ipca_mensal",
        # PIB trimestral (índice de volume)
        22109: "pib_trimestral_indice",
        # Taxa de inadimplência PF
        21084: "inadimplencia_PF",
        # Taxa de inadimplência PJ
        21085: "inadimplencia_PJ",
        # Saldo crédito PF - Brasil (para comparação)
        20539: "credito_PF_brasil",
        # Saldo crédito PJ - Brasil (para comparação)
        20541: "credito_PJ_brasil",
    }

    @staticmethod
    def coletar_serie(codigo_serie: int, nome: str,
                      data_inicio: str = "01/01/2015",
                      data_fim: str = "31/12/2025") -> pd.DataFrame:
        """Coleta uma série do SGS/BACEN."""
        url = BacenSGS.BASE_URL.format(serie=codigo_serie)
        params = {
            "dataInicial": data_inicio,
            "dataFinal": data_fim,
        }
        logger.info(f"BACEN-SGS: Coletando série {codigo_serie} ({nome})...")
        resp = safe_request(url, params=params)
        if resp is None:
            logger.error(f"Falha ao coletar série {codigo_serie}")
            return pd.DataFrame()

        try:
            data = resp.json()
        except Exception:
            logger.error(f"Resposta não-JSON para série {codigo_serie}")
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning(f"Série {codigo_serie} retornou vazia.")
            return df

        df.rename(columns={"data": "data", "valor": "valor"}, inplace=True)
        df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
        df["serie_codigo"] = codigo_serie
        df["serie_nome"] = nome
        return df

    @classmethod
    def coletar_todas(cls) -> pd.DataFrame:
        """Coleta todas as séries configuradas."""
        frames = []
        for codigo, nome in cls.SERIES.items():
            df = cls.coletar_serie(codigo, nome)
            if not df.empty:
                frames.append(df)
            time.sleep(1)  # rate limit

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "bacen_sgs_series")

        # Pivot para formato wide (útil para análise)
        df_wide = df_all.pivot_table(
            index="data", columns="serie_nome", values="valor"
        ).reset_index()
        save_dataframe(df_wide, "bacen_sgs_wide")

        return df_all


# ==============================================================================
# 2. PORTAL DA TRANSPARÊNCIA - TRANSFERÊNCIAS FEDERAIS
# ==============================================================================

class PortalTransparencia:
    """
    Coleta de transferências federais via Portal da Transparência.

    API: http://api.portaldatransparencia.gov.br/

    NOTA: A API requer cadastro e chave (API Key) gratuita.
    Cadastre-se em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email

    Sem a chave, o script faz download dos CSVs públicos disponíveis.
    """

    BASE_URL = "https://api.portaldatransparencia.gov.br/api-de-dados"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get("PORTAL_TRANSPARENCIA_API_KEY")
        self.headers = {}
        if self.api_key:
            self.headers["chave-api-dados"] = self.api_key
            logger.info("Portal da Transparência: API Key configurada.")
        else:
            logger.warning(
                "Portal da Transparência: Sem API Key. "
                "Cadastre-se em https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email "
                "e configure via env var PORTAL_TRANSPARENCIA_API_KEY ou parâmetro api_key."
            )

    def coletar_bolsa_familia_por_estado(self, ano: int, mes: int, uf: str) -> pd.DataFrame:
        """
        Coleta dados do Bolsa Família / Auxílio Brasil por UF via API.
        Endpoint: /bolsa-familia-por-municipio
        """
        if not self.api_key:
            logger.warning("API Key necessária para esta consulta.")
            return pd.DataFrame()

        url = f"{self.BASE_URL}/bolsa-familia-por-municipio"
        params = {
            "mesAno": f"{ano:04d}{mes:02d}",
            "codigoIbge": ESTADOS_NE[uf]["cod_ibge"],
            "pagina": 1,
        }
        all_records = []
        while True:
            resp = safe_request(url, params=params, headers=self.headers)
            if resp is None or not resp.json():
                break
            records = resp.json()
            all_records.extend(records)
            if len(records) < 15:  # página padrão
                break
            params["pagina"] += 1
            time.sleep(0.5)

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        df["uf"] = uf
        df["ano"] = ano
        df["mes"] = mes
        return df

    def coletar_transferencias_por_uf(self, ano: int, mes: int, uf: str) -> pd.DataFrame:
        """
        Coleta transferências federais (todas as modalidades) por UF.
        Endpoint: /transferencias/por-unidade-federativa
        """
        if not self.api_key:
            return pd.DataFrame()

        url = f"{self.BASE_URL}/transferencias"
        params = {
            "mesAno": f"{ano:04d}{mes:02d}",
            "codigoUF": ESTADOS_NE[uf]["cod_ibge"],
            "pagina": 1,
        }
        all_records = []
        while True:
            resp = safe_request(url, params=params, headers=self.headers)
            if resp is None:
                break
            data = resp.json()
            if not data:
                break
            all_records.extend(data)
            if len(data) < 15:
                break
            params["pagina"] += 1
            time.sleep(0.5)

        if not all_records:
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        df["uf"] = uf
        return df


# ==============================================================================
# 3. SICONFI / STN - DADOS FISCAIS
# ==============================================================================

class Siconfi:
    """
    Coleta dados fiscais do SICONFI (Tesouro Nacional).

    API pública: https://apidatalake.tesouro.gov.br/ords/siconfi/tt/

    Principais endpoints:
    - rreo: Relatório Resumido de Execução Orçamentária
    - rgf: Relatório de Gestão Fiscal
    - dca: Declaração de Contas Anuais
    - rreo_despesa_funcao: Despesa por função
    """

    BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

    @staticmethod
    def coletar_rreo(ano: int, periodo: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """
        Coleta RREO (Relatório Resumido de Execução Orçamentária).
        Contém dados de receitas e despesas, incluindo transferências recebidas.

        periodo: 1 a 6 (bimestral)
        """
        url = f"{Siconfi.BASE_URL}/rreo"
        params = {
            "an_exercicio": ano,
            "nr_periodo": periodo,
            "co_tipo_demonstrativo": "RREO",
            "id_ente": cod_ibge,
        }
        logger.info(f"SICONFI RREO: {uf} {ano} período {periodo}...")
        resp = safe_request(url, params=params, timeout=90)
        if resp is None:
            return pd.DataFrame()

        data = resp.json()
        items = data.get("items", [])
        if not items:
            logger.warning(f"SICONFI RREO vazio: {uf} {ano} P{periodo}")
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["uf"] = uf
        return df

    @staticmethod
    def coletar_rgf(ano: int, periodo: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """
        Coleta RGF (Relatório de Gestão Fiscal).
        Contém dados de dívida, operações de crédito, garantias.

        periodo: 1 a 3 (quadrimestral)
        """
        url = f"{Siconfi.BASE_URL}/rgf"
        params = {
            "an_exercicio": ano,
            "nr_periodo": periodo,
            "in_periodicidade": "Q",
            "co_tipo_demonstrativo": "RGF",
            "id_ente": cod_ibge,
            "co_poder": "E",
        }
        logger.info(f"SICONFI RGF: {uf} {ano} período {periodo}...")
        resp = safe_request(url, params=params, timeout=90)
        if resp is None:
            return pd.DataFrame()

        data = resp.json()
        items = data.get("items", [])
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["uf"] = uf
        return df

    @staticmethod
    def coletar_dca(ano: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """
        Coleta DCA (Declaração de Contas Anuais).
        Contém balanço patrimonial e demonstrações contábeis anuais.
        """
        url = f"{Siconfi.BASE_URL}/dca"
        params = {
            "an_exercicio": ano,
            "id_ente": cod_ibge,
        }
        logger.info(f"SICONFI DCA: {uf} {ano}...")
        resp = safe_request(url, params=params, timeout=90)
        if resp is None:
            return pd.DataFrame()

        data = resp.json()
        items = data.get("items", [])
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["uf"] = uf
        return df

    @classmethod
    def coletar_rreo_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta RREO de todos os estados do NE para todos os anos."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="SICONFI RREO - Anos"):
            for uf, info in ESTADOS_NE.items():
                for periodo in range(1, 7):  # 6 bimestres
                    df = cls.coletar_rreo(ano, periodo, info["cod_ibge"], uf)
                    if not df.empty:
                        frames.append(df)
                    time.sleep(1)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siconfi_rreo_nordeste")
        return df_all

    @classmethod
    def coletar_rgf_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta RGF de todos os estados do NE."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="SICONFI RGF - Anos"):
            for uf, info in ESTADOS_NE.items():
                for periodo in range(1, 4):  # 3 quadrimestres
                    df = cls.coletar_rgf(ano, periodo, info["cod_ibge"], uf)
                    if not df.empty:
                        frames.append(df)
                    time.sleep(1)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siconfi_rgf_nordeste")
        return df_all

    @classmethod
    def coletar_dca_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta DCA de todos os estados do NE."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="SICONFI DCA - Anos"):
            for uf, info in ESTADOS_NE.items():
                df = cls.coletar_dca(ano, info["cod_ibge"], uf)
                if not df.empty:
                    frames.append(df)
                time.sleep(1)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "siconfi_dca_nordeste")
        return df_all


# ==============================================================================
# 4. DADOS ABERTOS - BOLSA FAMÍLIA / AUXÍLIO BRASIL
# ==============================================================================

class BolsaFamilia:
    """
    Coleta dados do Bolsa Família / Auxílio Brasil / Novo Bolsa Família
    via Portal de Dados Abertos e Vis Data MDS.

    Fontes:
    - https://dados.gov.br/ (datasets de transferência de renda)
    - https://aplicacoes.mds.gov.br/sagi/vis/data3/ (Vis Data)
    - API SAGI: https://aplicacoes.mds.gov.br/sagi/servicos/misocial/
    """

    # API do SAGI/MDS para dados do Bolsa Família por município
    SAGI_URL = "https://aplicacoes.mds.gov.br/sagi/servicos/misocial"

    @staticmethod
    def coletar_via_api_sagi(ano: int, mes: int, cod_ibge_uf: str) -> pd.DataFrame:
        """
        Tenta coletar dados do MDS/SAGI.
        NOTA: Esta API pode ter restrições ou estar indisponível.
        Alternativa: download manual dos CSVs em dados.gov.br
        """
        url = f"{BolsaFamilia.SAGI_URL}"
        params = {
            "ano": ano,
            "mes": mes,
            "codigo_ibge": cod_ibge_uf,
            "tipo": "1",  # Bolsa Família
        }
        resp = safe_request(url, params=params, timeout=30)
        if resp is None:
            return pd.DataFrame()

        try:
            data = resp.json()
            return pd.DataFrame(data) if data else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def gerar_urls_download_dados_abertos() -> list[dict]:
        """
        Gera URLs para download dos datasets do Bolsa Família no dados.gov.br.
        Os arquivos CSV estão disponíveis mensalmente por UF.

        NOTA: As URLs mudam conforme o programa vigente:
        - 2015-2021: Bolsa Família
        - 2021-2023: Auxílio Brasil
        - 2023+: Novo Bolsa Família
        """
        urls = []
        base_msg = (
            "Datasets disponíveis em dados.gov.br:\n"
            "  - Bolsa Família (2015-2021): https://dados.gov.br/dados/conjuntos-dados/bolsa-familia-pagamentos\n"
            "  - Auxílio Brasil (2021-2023): https://dados.gov.br/dados/conjuntos-dados/auxilio-brasil\n"
            "  - Novo Bolsa Família (2023+): https://dados.gov.br/dados/conjuntos-dados/bolsa-familia-pagamentos\n"
        )
        logger.info(base_msg)

        # URLs conhecidas para download direto (Portal da Transparência - arquivos CSV)
        for ano in range(PERIODO_INICIO, PERIODO_FIM + 1):
            for mes in range(1, 13):
                urls.append({
                    "ano": ano,
                    "mes": mes,
                    "url": f"https://portaldatransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/{ano:04d}{mes:02d}",
                    "descricao": f"Bolsa Família {ano:04d}/{mes:02d}",
                })
        return urls


# ==============================================================================
# 5. TRANSFERÊNCIAS CONSTITUCIONAIS (FPE, FPM, FUNDEB)
# ==============================================================================

class TransferenciasConstitucionais:
    """
    Extrai dados de transferências constitucionais do RREO (SICONFI).

    O RREO (Anexo 01 - Balanço Orçamentário) contém as receitas de
    transferências correntes e de capital recebidas pelos estados,
    incluindo FPE, FPM, FUNDEB, SUS, FNDE, etc.

    Fonte primária: SICONFI/RREO já coletado.
    Fonte complementar: RREO-Anexo 06 (Resultado Primário) detalha
    transferências constitucionais e legais.
    """

    BASE_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"

    ANEXOS_TRANSFERENCIAS = [
        "RREO-Anexo 01",
        "RREO-Anexo 06",
    ]

    TERMOS_TRANSFERENCIA = [
        "transfer",
        "fpe",
        "fpm",
        "fundeb",
        "fundef",
        "cota-parte",
        "cide",
        "royalties",
        "compensações financeiras",
    ]

    @classmethod
    def coletar_transferencias_rreo(cls, ano: int, cod_ibge: str, uf: str) -> pd.DataFrame:
        """Coleta dados de transferências via RREO (Anexos 01 e 06)."""
        frames = []
        for anexo in cls.ANEXOS_TRANSFERENCIAS:
            for periodo in range(1, 7):
                url = cls.BASE_URL
                params = {
                    "an_exercicio": ano,
                    "nr_periodo": periodo,
                    "co_tipo_demonstrativo": "RREO",
                    "no_anexo": anexo,
                    "id_ente": cod_ibge,
                }
                logger.info(f"Transferências ({anexo}): {uf} {ano} P{periodo}...")
                resp = safe_request(url, params=params, timeout=90)
                if resp is None:
                    continue

                data = resp.json()
                items = data.get("items", [])
                if not items:
                    continue

                df = pd.DataFrame(items)
                termos = "|".join(cls.TERMOS_TRANSFERENCIA)
                mask = df["conta"].str.lower().str.contains(termos, na=False)
                df_transf = df[mask].copy()
                if not df_transf.empty:
                    df_transf["uf"] = uf
                    frames.append(df_transf)
                time.sleep(1)

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @classmethod
    def coletar_nordeste(cls, anos: range = None) -> pd.DataFrame:
        """Coleta transferências de todos os estados do NE via RREO."""
        if anos is None:
            anos = range(PERIODO_INICIO, PERIODO_FIM + 1)

        frames = []
        for ano in tqdm(anos, desc="Transferências Constitucionais"):
            for uf, info in ESTADOS_NE.items():
                df = cls.coletar_transferencias_rreo(ano, info["cod_ibge"], uf)
                if not df.empty:
                    frames.append(df)

        if not frames:
            return pd.DataFrame()

        df_all = pd.concat(frames, ignore_index=True)
        save_dataframe(df_all, "transferencias_constitucionais_nordeste")
        return df_all


# ==============================================================================
# 6. ORQUESTRADOR PRINCIPAL
# ==============================================================================

class PipelineColeta:
    """Orquestrador da pipeline de coleta de dados."""

    def __init__(self, api_key_portal: str = None):
        self.portal = PortalTransparencia(api_key=api_key_portal)
        self.resumo = {}

    def executar(self, modulos: list[str] = None):
        """
        Executa a coleta completa ou parcial.

        modulos: lista de módulos a executar. Se None, executa todos.
        Opções: ["bacen", "siconfi_rreo", "siconfi_rgf", "siconfi_dca",
                 "transferencias", "bolsa_familia"]
        """
        if modulos is None:
            modulos = ["bacen", "siconfi_rreo", "siconfi_rgf", "siconfi_dca", "transferencias"]

        logger.info("=" * 70)
        logger.info("PIPELINE DE COLETA DE DADOS - TESE DESP/UFC")
        logger.info(f"Módulos: {modulos}")
        logger.info(f"Período: {PERIODO_INICIO}-{PERIODO_FIM}")
        logger.info(f"Diretório: {BASE_DIR.absolute()}")
        logger.info("=" * 70)

        inicio = datetime.now()

        # --- BACEN SGS ---
        if "bacen" in modulos:
            logger.info("\n>>> MÓDULO 1: BACEN-SGS <<<")
            df = BacenSGS.coletar_todas()
            self.resumo["bacen"] = len(df)

        # --- SICONFI RREO ---
        if "siconfi_rreo" in modulos:
            logger.info("\n>>> MÓDULO 2: SICONFI - RREO <<<")
            df = Siconfi.coletar_rreo_nordeste()
            self.resumo["siconfi_rreo"] = len(df)

        # --- SICONFI RGF ---
        if "siconfi_rgf" in modulos:
            logger.info("\n>>> MÓDULO 3: SICONFI - RGF <<<")
            df = Siconfi.coletar_rgf_nordeste()
            self.resumo["siconfi_rgf"] = len(df)

        # --- SICONFI DCA ---
        if "siconfi_dca" in modulos:
            logger.info("\n>>> MÓDULO 4: SICONFI - DCA <<<")
            df = Siconfi.coletar_dca_nordeste()
            self.resumo["siconfi_dca"] = len(df)

        # --- Transferências Constitucionais ---
        if "transferencias" in modulos:
            logger.info("\n>>> MÓDULO 5: Transferências Constitucionais <<<")
            df = TransferenciasConstitucionais.coletar_nordeste()
            self.resumo["transferencias"] = len(df)

        # --- Bolsa Família (gera URLs para download) ---
        if "bolsa_familia" in modulos:
            logger.info("\n>>> MÓDULO 6: Bolsa Família / Auxílio Brasil <<<")
            urls = BolsaFamilia.gerar_urls_download_dados_abertos()
            urls_df = pd.DataFrame(urls)
            save_dataframe(urls_df, "bolsa_familia_urls_download")
            self.resumo["bolsa_familia_urls"] = len(urls)

        # --- Resumo Final ---
        fim = datetime.now()
        duracao = fim - inicio
        logger.info("\n" + "=" * 70)
        logger.info("RESUMO DA COLETA")
        logger.info("=" * 70)
        for modulo, qtd in self.resumo.items():
            logger.info(f"  {modulo}: {qtd} registros")
        logger.info(f"  Duração total: {duracao}")
        logger.info(f"  Dados salvos em: {BASE_DIR.absolute()}")
        logger.info("=" * 70)

        # Salva metadados da execução
        meta = {
            "data_execucao": fim.isoformat(),
            "duracao_segundos": duracao.total_seconds(),
            "periodo": f"{PERIODO_INICIO}-{PERIODO_FIM}",
            "estados": list(ESTADOS_NE.keys()),
            "modulos_executados": modulos,
            "resumo": self.resumo,
        }
        meta_path = BASE_DIR / "metadata_coleta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        logger.info(f"Metadados: {meta_path}")

        return self.resumo


# ==============================================================================
# EXECUÇÃO
# ==============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Pipeline de coleta de dados públicos - Tese DESP/UFC"
    )
    parser.add_argument(
        "--modulos",
        nargs="+",
        default=None,
        choices=["bacen", "siconfi_rreo", "siconfi_rgf", "siconfi_dca",
                 "transferencias", "bolsa_familia"],
        help="Módulos a executar (default: todos exceto bolsa_familia)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="API Key do Portal da Transparência",
    )
    parser.add_argument(
        "--apenas-bacen",
        action="store_true",
        help="Executa apenas a coleta do BACEN-SGS (rápido, para teste)",
    )

    args = parser.parse_args()

    if args.apenas_bacen:
        modulos = ["bacen"]
    else:
        modulos = args.modulos

    pipeline = PipelineColeta(api_key_portal=args.api_key)
    pipeline.executar(modulos=modulos)

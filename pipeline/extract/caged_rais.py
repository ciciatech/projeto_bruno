"""
Extrator de dados do CAGED e RAIS via FTP do MTE/PDET.

Fonte: Ministério do Trabalho e Emprego — PDET
FTP: ftp://ftp.mtps.gov.br/pdet/microdados/
  - CAGED Antigo (2015–2019): CAGED/{ano}/CAGEDEST_{MMAA}.7z
  - Novo CAGED (2020+):       NOVO CAGED/{ano}/{anoMM}/CAGEDMOV{anoMM}.7z
  - RAIS:                      RAIS/{ano}/RAIS_VINC_PUB_NORDESTE.7z

Pré-requisitos:
  pip install py7zr
"""

import logging
import tempfile
from ftplib import FTP, error_perm
from pathlib import Path

import pandas as pd

from pipeline.config import PERIODO_INICIO, PERIODO_FIM, ESTADOS_NE, RAW_DIR
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

FTP_HOST = "ftp.mtps.gov.br"
CAGED_ANTIGO_FTP_BASE = "/pdet/microdados/CAGED"
CAGED_FTP_BASE = "/pdet/microdados/NOVO CAGED"
RAIS_FTP_BASE = "/pdet/microdados/RAIS"

# Códigos IBGE (2 dígitos) das UFs do Nordeste
UFS_NE_IBGE = {int(info["cod_ibge"]) for info in ESTADOS_NE.values()}
UF_IBGE_SIGLA = {int(info["cod_ibge"]): uf for uf, info in ESTADOS_NE.items()}


# =============================================================================
# Helpers FTP e 7z
# =============================================================================


def _ftp_download(remote_path: str, local_path: str):
    """Baixa arquivo do FTP do MTE."""
    logger.info(f"FTP download: {FTP_HOST}{remote_path}")
    with FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    logger.info(f"Download concluído: {Path(local_path).name}")


def _read_7z(archive_path: str, extract_dir: str, sep: str = ";", encoding: str = "latin-1") -> pd.DataFrame:
    """Extrai .7z para diretório temporário e lê o primeiro CSV/TXT."""
    import py7zr

    with py7zr.SevenZipFile(archive_path, "r") as z:
        names = z.getnames()
        data_file = next(
            (n for n in names if n.lower().endswith((".csv", ".txt"))), None
        )
        if data_file is None:
            raise ValueError(f"Nenhum CSV/TXT em {archive_path}: {names}")

        logger.info(f"Extraindo {data_file}...")
        z.extractall(path=extract_dir)

    file_path = Path(extract_dir) / data_file
    try:
        return pd.read_csv(file_path, sep=sep, encoding=encoding, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(file_path, sep=sep, encoding="utf-8", low_memory=False)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas do CAGED (variações entre anos)."""
    logger.info(f"Colunas originais: {list(df.columns)}")
    mapping = {}
    for col in df.columns:
        lc = col.lower().strip()
        if "competencia" in lc or "competência" in lc:
            mapping[col] = "competencia"
        elif lc == "uf":
            mapping[col] = "uf"
        elif "municipio" in lc or "município" in lc:
            mapping[col] = "municipio"
        elif "saldomov" in lc:
            mapping[col] = "saldo_movimentacao"
        elif lc in ("salário", "salario", "valorsaláriofixo", "valorsalariofixo"):
            mapping[col] = "salario"
        elif "salario" in lc or "salário" in lc:
            mapping[col] = "salario"
        elif "grau" in lc and "instru" in lc:
            mapping[col] = "grau_instrucao"
        elif lc == "sexo":
            mapping[col] = "sexo"
        elif "raca" in lc or "raça" in lc:
            mapping[col] = "raca_cor"
        elif lc == "idade":
            mapping[col] = "idade"
        elif "cbo" in lc and "ocupa" in lc:
            mapping[col] = "cbo_2002"
        elif lc == "subclasse":
            mapping[col] = "cnae_subclasse"
        elif lc in ("seção", "secao", "seçao"):
            mapping[col] = "secao_cnae"
        elif lc in ("região", "regiao"):
            mapping[col] = "regiao"
    logger.info(f"Mapeamento: {mapping}")
    return df.rename(columns=mapping)


def _filtrar_nordeste(df: pd.DataFrame) -> pd.DataFrame:
    """Filtra registros do Nordeste por UF ou região."""
    if "uf" in df.columns:
        df["uf"] = pd.to_numeric(df["uf"], errors="coerce")
        return df[df["uf"].isin(UFS_NE_IBGE)].copy()
    if "regiao" in df.columns:
        df["regiao"] = pd.to_numeric(df["regiao"], errors="coerce")
        return df[df["regiao"] == 2].copy()
    return df


# =============================================================================
# Classe principal
# =============================================================================


class CagedRais:
    """Extrator de dados CAGED e RAIS via FTP do MTE/PDET."""

    # =========================================================================
    # CAGED Antigo (2015–2019) — layout pré-eSocial
    # =========================================================================

    @staticmethod
    def _processar_caged_antigo_mes(ano: int, mes: int) -> pd.DataFrame | None:
        """Baixa e processa microdados do CAGED Antigo (pré-2020). Retorna dados NE."""
        competencia = f"{mes:02d}{ano}"
        remote_path = f"{CAGED_ANTIGO_FTP_BASE}/{ano}/CAGEDEST_{competencia}.7z"

        with tempfile.TemporaryDirectory() as tmpdir:
            local_7z = Path(tmpdir) / f"CAGEDEST_{competencia}.7z"

            try:
                _ftp_download(remote_path, str(local_7z))
            except error_perm as e:
                logger.warning(f"CAGED Antigo {ano}/{mes:02d}: não disponível ({e})")
                return None
            except Exception as e:
                logger.warning(f"CAGED Antigo {ano}/{mes:02d}: erro download ({e})")
                return None

            try:
                df = _read_7z(str(local_7z), tmpdir)
            except Exception as e:
                logger.error(f"CAGED Antigo {ano}/{mes:02d}: erro ao extrair ({e})")
                return None

        logger.info(f"CAGED Antigo {ano}/{mes:02d} colunas: {list(df.columns)}")

        # Normalizar colunas do layout antigo
        col_map = {}
        for col in df.columns:
            lc = col.lower().strip()
            if lc in ("município", "municipio"):
                col_map[col] = "municipio"
            elif lc == "uf":
                col_map[col] = "uf"
            elif lc in ("região", "regiao"):
                col_map[col] = "regiao"
            elif lc in ("admitidos/desligados", "admitidosdesligados", "tipo_mov"):
                col_map[col] = "admitidos_desligados"
            elif lc in ("salário", "salario", "salmensal"):
                col_map[col] = "salario"
            elif "grau" in lc and "instru" in lc:
                col_map[col] = "grau_instrucao"
            elif lc == "sexo":
                col_map[col] = "sexo"
            elif "raca" in lc or "raça" in lc:
                col_map[col] = "raca_cor"
            elif lc in ("subatividade ibge", "subclasse", "cnae20subclasse", "cnae_2_subclasse"):
                col_map[col] = "cnae_subclasse"
            elif lc in ("seção", "secao", "seçao"):
                col_map[col] = "secao_cnae"

        df.rename(columns=col_map, inplace=True)

        # Filtrar Nordeste
        df = _filtrar_nordeste(df)
        if df.empty:
            logger.warning(f"CAGED Antigo {ano}/{mes:02d}: sem dados Nordeste.")
            return None

        # Mapear admitidos_desligados → saldo_movimentacao (1=admissão → +1, 2=desligamento → -1)
        if "admitidos_desligados" in df.columns:
            df["admitidos_desligados"] = pd.to_numeric(df["admitidos_desligados"], errors="coerce")
            df["saldo_movimentacao"] = df["admitidos_desligados"].map({1: 1, 2: -1, 3: -1})
            df["saldo_movimentacao"] = df["saldo_movimentacao"].fillna(0).astype(int)

        if "uf" in df.columns:
            df["uf"] = pd.to_numeric(df["uf"], errors="coerce")
            df["sigla_uf"] = df["uf"].map(UF_IBGE_SIGLA)

        df["ano"] = ano
        df["mes"] = mes

        if "salario" in df.columns:
            df["salario"] = (
                df["salario"]
                .astype(str)
                .str.replace(",", ".", regex=False)
            )
            df["salario"] = pd.to_numeric(df["salario"], errors="coerce")

        logger.info(f"CAGED Antigo {ano}/{mes:02d}: {len(df)} registros Nordeste.")
        return df

    @classmethod
    def coletar_caged_antigo_nordeste(
        cls, ano_inicio: int = PERIODO_INICIO, ano_fim: int = 2019
    ) -> pd.DataFrame | None:
        """
        Coleta CAGED Antigo (2015–2019) e gera 3 arquivos agregados:
          - caged_antigo_saldo_mensal.csv  (UF + mês)
          - caged_antigo_por_setor.csv     (UF + ano + divisão CNAE)
          - caged_antigo_por_perfil.csv    (UF + ano + sexo + escolaridade)
        """
        cache = RAW_DIR / "caged" / "nordeste" / "caged_antigo_saldo_mensal.csv"
        if cache.exists():
            logger.info("CAGED Antigo Nordeste: cache encontrado, pulando coleta.")
            return pd.read_csv(cache)

        saldo_frames = []
        setor_frames = []
        perfil_frames = []

        for ano in range(ano_inicio, ano_fim + 1):
            for mes in range(1, 13):
                df = cls._processar_caged_antigo_mes(ano, mes)
                if df is None:
                    continue

                # --- Saldo mensal por UF ---
                if "sigla_uf" in df.columns and "saldo_movimentacao" in df.columns:
                    agg1 = df.groupby(["ano", "mes", "sigla_uf"], as_index=False).agg(
                        admissoes=("saldo_movimentacao", lambda x: (x == 1).sum()),
                        desligamentos=("saldo_movimentacao", lambda x: (x == -1).sum()),
                        saldo=("saldo_movimentacao", "sum"),
                        salario_medio=("salario", "mean")
                        if "salario" in df.columns
                        else ("saldo_movimentacao", "count"),
                        total_movimentacoes=("saldo_movimentacao", "count"),
                    )
                    saldo_frames.append(agg1)

                # --- Por setor ---
                cnae_col = (
                    "secao_cnae" if "secao_cnae" in df.columns
                    else "cnae_subclasse" if "cnae_subclasse" in df.columns
                    else None
                )
                if cnae_col and "sigla_uf" in df.columns:
                    if cnae_col == "cnae_subclasse":
                        df["divisao_cnae"] = df[cnae_col].astype(str).str[:2]
                    else:
                        df["divisao_cnae"] = df[cnae_col].astype(str)

                    agg2 = df.groupby(
                        ["ano", "sigla_uf", "divisao_cnae"], as_index=False
                    ).agg(
                        saldo=("saldo_movimentacao", "sum"),
                        salario_medio=("salario", "mean")
                        if "salario" in df.columns
                        else ("saldo_movimentacao", "count"),
                        total_movimentacoes=("saldo_movimentacao", "count"),
                    )
                    setor_frames.append(agg2)

                # --- Por perfil ---
                if (
                    "sigla_uf" in df.columns
                    and "sexo" in df.columns
                    and "grau_instrucao" in df.columns
                ):
                    agg3 = df.groupby(
                        ["ano", "sigla_uf", "sexo", "grau_instrucao"], as_index=False
                    ).agg(
                        admissoes=("saldo_movimentacao", lambda x: (x == 1).sum()),
                        desligamentos=("saldo_movimentacao", lambda x: (x == -1).sum()),
                        saldo=("saldo_movimentacao", "sum"),
                        salario_medio=("salario", "mean")
                        if "salario" in df.columns
                        else ("saldo_movimentacao", "count"),
                        total_movimentacoes=("saldo_movimentacao", "count"),
                    )
                    perfil_frames.append(agg3)

        result = None

        if saldo_frames:
            df_saldo = pd.concat(saldo_frames, ignore_index=True)
            df_saldo.sort_values(["ano", "mes", "sigla_uf"], inplace=True)
            save_dataframe(df_saldo, "caged_antigo_saldo_mensal", path_parts=["caged", "nordeste"])
            result = df_saldo
            logger.info(f"CAGED Antigo saldo mensal: {len(df_saldo)} registros salvos.")

        if setor_frames:
            df_setor = pd.concat(setor_frames, ignore_index=True)
            df_setor = df_setor.groupby(
                ["ano", "sigla_uf", "divisao_cnae"], as_index=False
            ).agg(
                saldo=("saldo", "sum"),
                salario_medio=("salario_medio", "mean"),
                total_movimentacoes=("total_movimentacoes", "sum"),
            )
            df_setor.sort_values(["ano", "sigla_uf", "divisao_cnae"], inplace=True)
            save_dataframe(df_setor, "caged_antigo_por_setor", path_parts=["caged", "nordeste"])
            logger.info(f"CAGED Antigo por setor: {len(df_setor)} registros salvos.")

        if perfil_frames:
            df_perfil = pd.concat(perfil_frames, ignore_index=True)
            df_perfil = df_perfil.groupby(
                ["ano", "sigla_uf", "sexo", "grau_instrucao"], as_index=False
            ).agg(
                admissoes=("admissoes", "sum"),
                desligamentos=("desligamentos", "sum"),
                saldo=("saldo", "sum"),
                salario_medio=("salario_medio", "mean"),
                total_movimentacoes=("total_movimentacoes", "sum"),
            )
            df_perfil.sort_values(["ano", "sigla_uf", "sexo", "grau_instrucao"], inplace=True)
            save_dataframe(df_perfil, "caged_antigo_por_perfil", path_parts=["caged", "nordeste"])
            logger.info(f"CAGED Antigo por perfil: {len(df_perfil)} registros salvos.")

        if result is None:
            logger.error("CAGED Antigo: nenhum dado coletado.")
        return result

    # =========================================================================
    # CAGED — Novo CAGED (a partir de jan/2020)
    # =========================================================================

    @staticmethod
    def _processar_caged_mes(ano: int, mes: int) -> pd.DataFrame | None:
        """Baixa e processa microdados CAGED de um mês. Retorna dados do NE filtrados."""
        competencia = f"{ano}{mes:02d}"
        remote_path = f"{CAGED_FTP_BASE}/{ano}/{competencia}/CAGEDMOV{competencia}.7z"

        with tempfile.TemporaryDirectory() as tmpdir:
            local_7z = Path(tmpdir) / f"CAGEDMOV{competencia}.7z"

            try:
                _ftp_download(remote_path, str(local_7z))
            except error_perm as e:
                logger.warning(f"CAGED {competencia}: arquivo não disponível ({e})")
                return None
            except Exception as e:
                logger.warning(f"CAGED {competencia}: erro no download ({e})")
                return None

            try:
                df = _read_7z(str(local_7z), tmpdir)
            except Exception as e:
                logger.error(f"CAGED {competencia}: erro ao extrair ({e})")
                return None

        df = _normalize_columns(df)
        df = _filtrar_nordeste(df)

        if df.empty:
            logger.warning(f"CAGED {competencia}: sem dados Nordeste.")
            return None

        # Adicionar sigla_uf e período
        if "uf" in df.columns:
            df["sigla_uf"] = df["uf"].map(UF_IBGE_SIGLA)
        df["ano"] = ano
        df["mes"] = mes

        # Converter numéricos
        if "saldo_movimentacao" in df.columns:
            df["saldo_movimentacao"] = pd.to_numeric(
                df["saldo_movimentacao"], errors="coerce"
            )
        if "salario" in df.columns:
            df["salario"] = pd.to_numeric(df["salario"], errors="coerce")

        logger.info(f"CAGED {competencia}: {len(df)} registros Nordeste.")
        return df

    @classmethod
    def coletar_caged_nordeste(
        cls, ano_inicio: int = 2020, ano_fim: int = PERIODO_FIM
    ) -> pd.DataFrame | None:
        """
        Coleta Novo CAGED e gera 3 arquivos agregados para o Nordeste:
          - caged_saldo_mensal.csv  (UF + mês)
          - caged_por_setor.csv    (UF + ano + seção CNAE)
          - caged_por_perfil.csv   (UF + ano + sexo + escolaridade)
        """
        # Verificar cache principal
        cache = RAW_DIR / "caged" / "nordeste" / "caged_saldo_mensal.csv"
        if cache.exists():
            logger.info("CAGED Nordeste: cache encontrado, pulando coleta.")
            return pd.read_csv(cache)

        saldo_frames = []
        setor_frames = []
        perfil_frames = []

        for ano in range(ano_inicio, ano_fim + 1):
            for mes in range(1, 13):
                df = cls._processar_caged_mes(ano, mes)
                if df is None:
                    continue

                # --- Agregação 1: saldo mensal por UF ---
                if "sigla_uf" in df.columns and "saldo_movimentacao" in df.columns:
                    agg1 = df.groupby(["ano", "mes", "sigla_uf"], as_index=False).agg(
                        admissoes=("saldo_movimentacao", lambda x: (x == 1).sum()),
                        desligamentos=("saldo_movimentacao", lambda x: (x == -1).sum()),
                        saldo=("saldo_movimentacao", "sum"),
                        salario_medio=("salario", "mean")
                        if "salario" in df.columns
                        else ("saldo_movimentacao", "count"),
                        total_movimentacoes=("saldo_movimentacao", "count"),
                    )
                    saldo_frames.append(agg1)

                # --- Agregação 2: por setor (seção CNAE) ---
                cnae_col = (
                    "secao_cnae"
                    if "secao_cnae" in df.columns
                    else "cnae_subclasse"
                    if "cnae_subclasse" in df.columns
                    else None
                )
                if cnae_col and "sigla_uf" in df.columns:
                    if cnae_col == "cnae_subclasse":
                        df["divisao_cnae"] = df[cnae_col].astype(str).str[:2]
                    else:
                        df["divisao_cnae"] = df[cnae_col].astype(str)

                    agg2 = df.groupby(
                        ["ano", "sigla_uf", "divisao_cnae"], as_index=False
                    ).agg(
                        saldo=("saldo_movimentacao", "sum"),
                        salario_medio=("salario", "mean")
                        if "salario" in df.columns
                        else ("saldo_movimentacao", "count"),
                        total_movimentacoes=("saldo_movimentacao", "count"),
                    )
                    setor_frames.append(agg2)

                # --- Agregação 3: por perfil (sexo + escolaridade) ---
                if (
                    "sigla_uf" in df.columns
                    and "sexo" in df.columns
                    and "grau_instrucao" in df.columns
                ):
                    agg3 = df.groupby(
                        ["ano", "sigla_uf", "sexo", "grau_instrucao"], as_index=False
                    ).agg(
                        admissoes=("saldo_movimentacao", lambda x: (x == 1).sum()),
                        desligamentos=("saldo_movimentacao", lambda x: (x == -1).sum()),
                        saldo=("saldo_movimentacao", "sum"),
                        salario_medio=("salario", "mean")
                        if "salario" in df.columns
                        else ("saldo_movimentacao", "count"),
                        total_movimentacoes=("saldo_movimentacao", "count"),
                    )
                    perfil_frames.append(agg3)

        # --- Salvar consolidados ---
        result = None

        if saldo_frames:
            df_saldo = pd.concat(saldo_frames, ignore_index=True)
            df_saldo.sort_values(["ano", "mes", "sigla_uf"], inplace=True)
            save_dataframe(
                df_saldo, "caged_saldo_mensal", path_parts=["caged", "nordeste"]
            )
            result = df_saldo
            logger.info(f"CAGED saldo mensal: {len(df_saldo)} registros salvos.")

        if setor_frames:
            df_setor = pd.concat(setor_frames, ignore_index=True)
            # Re-agregar (meses foram somados separadamente)
            df_setor = df_setor.groupby(
                ["ano", "sigla_uf", "divisao_cnae"], as_index=False
            ).agg(
                saldo=("saldo", "sum"),
                salario_medio=("salario_medio", "mean"),
                total_movimentacoes=("total_movimentacoes", "sum"),
            )
            df_setor.sort_values(["ano", "sigla_uf", "divisao_cnae"], inplace=True)
            save_dataframe(
                df_setor, "caged_por_setor", path_parts=["caged", "nordeste"]
            )
            logger.info(f"CAGED por setor: {len(df_setor)} registros salvos.")

        if perfil_frames:
            df_perfil = pd.concat(perfil_frames, ignore_index=True)
            df_perfil = df_perfil.groupby(
                ["ano", "sigla_uf", "sexo", "grau_instrucao"], as_index=False
            ).agg(
                admissoes=("admissoes", "sum"),
                desligamentos=("desligamentos", "sum"),
                saldo=("saldo", "sum"),
                salario_medio=("salario_medio", "mean"),
                total_movimentacoes=("total_movimentacoes", "sum"),
            )
            df_perfil.sort_values(
                ["ano", "sigla_uf", "sexo", "grau_instrucao"], inplace=True
            )
            save_dataframe(
                df_perfil, "caged_por_perfil", path_parts=["caged", "nordeste"]
            )
            logger.info(f"CAGED por perfil: {len(df_perfil)} registros salvos.")

        if result is None:
            logger.error("CAGED: nenhum dado coletado.")
        return result

    # =========================================================================
    # RAIS — Relação Anual de Informações Sociais
    # =========================================================================

    # Colunas que precisamos da RAIS (ignora ~50 colunas desnecessárias)
    RAIS_COLUNAS_ALVO = {
        "Município": "municipio",
        "CNAE 2.0 Subclasse": "cnae_subclasse",
        "CNAE 2.0 Classe": "cnae_subclasse",
        "Vl Remun Média Nom": "remuneracao_media",
        "Qtd Hora Contr": "horas_contratadas",
        "Escolaridade após 2005": "grau_instrucao",
        "Sexo Trabalhador": "sexo",
        "Raça Cor": "raca_cor",
        "Vínculo Ativo 31/12": "vinculo_ativo",
    }

    @classmethod
    def _baixar_rais_ano(cls, ano: int, tmpdir: str) -> list[Path]:
        """
        Baixa arquivo(s) RAIS do FTP para um ano.
        2018+: arquivo consolidado RAIS_VINC_PUB_NORDESTE.7z
        2015-2017: arquivos individuais por UF (AL2015.7z, BA2015.7z, ...)
        Retorna lista de paths dos .7z baixados.
        """
        # Tentar arquivo consolidado Nordeste primeiro
        remote_consolidado = f"{RAIS_FTP_BASE}/{ano}/RAIS_VINC_PUB_NORDESTE.7z"
        local_consolidado = Path(tmpdir) / f"RAIS_VINC_PUB_NORDESTE_{ano}.7z"
        try:
            _ftp_download(remote_consolidado, str(local_consolidado))
            return [local_consolidado]
        except error_perm:
            logger.info(f"RAIS {ano}: arquivo consolidado Nordeste não existe, tentando por UF...")
        except Exception as e:
            logger.warning(f"RAIS {ano}: erro no download consolidado ({e}), tentando por UF...")

        # Fallback: baixar por UF individual
        arquivos = []
        siglas_ne = ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"]
        for uf in siglas_ne:
            remote_uf = f"{RAIS_FTP_BASE}/{ano}/{uf}{ano}.7z"
            local_uf = Path(tmpdir) / f"{uf}{ano}.7z"
            try:
                _ftp_download(remote_uf, str(local_uf))
                arquivos.append(local_uf)
            except error_perm:
                logger.warning(f"RAIS {ano}/{uf}: não disponível.")
            except Exception as e:
                logger.warning(f"RAIS {ano}/{uf}: erro download ({e})")

        return arquivos

    @classmethod
    def _ler_rais_chunked(cls, archive_path: str, extract_dir: str, ano: int) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Lê RAIS de um .7z em chunks para evitar OOM.
        Retorna (agg_vinculos_por_uf, agg_por_setor) já agregados.
        """
        import py7zr

        with py7zr.SevenZipFile(archive_path, "r") as z:
            names = z.getnames()
            data_file = next(
                (n for n in names if n.lower().endswith((".csv", ".txt"))), None
            )
            if data_file is None:
                raise ValueError(f"Nenhum CSV/TXT em {archive_path}: {names}")
            logger.info(f"Extraindo {data_file}...")
            z.extractall(path=extract_dir)

        file_path = Path(extract_dir) / data_file

        # Detectar colunas disponíveis lendo apenas o header
        try:
            header_df = pd.read_csv(file_path, sep=";", encoding="latin-1", nrows=0)
        except UnicodeDecodeError:
            header_df = pd.read_csv(file_path, sep=";", encoding="utf-8", nrows=0)

        all_cols = list(header_df.columns)
        logger.info(f"RAIS {ano} colunas: {all_cols}")

        # Mapear colunas disponíveis
        usecols = []
        col_map = {}
        for orig, target in cls.RAIS_COLUNAS_ALVO.items():
            if orig in all_cols and target not in col_map.values():
                usecols.append(orig)
                col_map[orig] = target

        if not usecols:
            raise ValueError(f"RAIS {ano}: nenhuma coluna conhecida encontrada.")

        logger.info(f"RAIS {ano} usando colunas: {usecols}")

        # Ler em chunks de 500k linhas
        vinculos_aggs = []
        setor_aggs = []
        chunk_size = 500_000
        total_rows = 0

        try:
            reader = pd.read_csv(
                file_path, sep=";", encoding="latin-1",
                usecols=usecols, low_memory=False, chunksize=chunk_size,
            )
        except UnicodeDecodeError:
            reader = pd.read_csv(
                file_path, sep=";", encoding="utf-8",
                usecols=usecols, low_memory=False, chunksize=chunk_size,
            )

        for chunk in reader:
            chunk.rename(columns=col_map, inplace=True)
            total_rows += len(chunk)

            # Filtrar Nordeste se tiver coluna municipio (arquivo consolidado)
            if "municipio" in chunk.columns:
                chunk["municipio"] = pd.to_numeric(chunk["municipio"], errors="coerce")
                chunk["uf_cod"] = chunk["municipio"] // 10000
                chunk = chunk[chunk["uf_cod"].isin(UFS_NE_IBGE)]
                chunk["sigla_uf"] = chunk["uf_cod"].map(UF_IBGE_SIGLA)
            else:
                # Arquivo por UF — extrair sigla do nome do arquivo
                arquivo_nome = Path(archive_path).stem
                uf_sigla = arquivo_nome[:2].upper()
                chunk["sigla_uf"] = uf_sigla

            if chunk.empty:
                continue

            chunk["ano"] = ano
            if "remuneracao_media" in chunk.columns:
                chunk["remuneracao_media"] = pd.to_numeric(chunk["remuneracao_media"], errors="coerce")

            # Agregação 1: vínculos por UF
            agg1 = chunk.groupby(["ano", "sigla_uf"], as_index=False).agg(
                vinculos_ativos=("ano", "count"),
                remuneracao_media=("remuneracao_media", "mean")
                if "remuneracao_media" in chunk.columns
                else ("ano", "count"),
            )
            vinculos_aggs.append(agg1)

            # Agregação 2: por setor
            if "cnae_subclasse" in chunk.columns:
                chunk["divisao_cnae"] = chunk["cnae_subclasse"].astype(str).str[:2]
                agg2 = chunk.groupby(
                    ["ano", "sigla_uf", "divisao_cnae"], as_index=False
                ).agg(
                    vinculos_ativos=("ano", "count"),
                    remuneracao_media=("remuneracao_media", "mean")
                    if "remuneracao_media" in chunk.columns
                    else ("ano", "count"),
                )
                setor_aggs.append(agg2)

        logger.info(f"RAIS {ano}: {total_rows} registros lidos em chunks.")

        # Re-agregar os chunks
        df_vinculos = pd.DataFrame()
        if vinculos_aggs:
            df_v = pd.concat(vinculos_aggs, ignore_index=True)
            df_vinculos = df_v.groupby(["ano", "sigla_uf"], as_index=False).agg(
                vinculos_ativos=("vinculos_ativos", "sum"),
                remuneracao_media=("remuneracao_media", "mean"),
            )

        df_setor = pd.DataFrame()
        if setor_aggs:
            df_s = pd.concat(setor_aggs, ignore_index=True)
            df_setor = df_s.groupby(["ano", "sigla_uf", "divisao_cnae"], as_index=False).agg(
                vinculos_ativos=("vinculos_ativos", "sum"),
                remuneracao_media=("remuneracao_media", "mean"),
            )

        return df_vinculos, df_setor

    @classmethod
    def coletar_rais_nordeste(
        cls, ano_inicio: int = PERIODO_INICIO, ano_fim: int = None
    ) -> pd.DataFrame | None:
        """
        Coleta RAIS Nordeste e gera 2 arquivos agregados:
          - rais_vinculos.csv     (UF + ano)
          - rais_por_setor.csv   (UF + ano + seção CNAE)

        Processa em chunks para evitar OOM (~12M linhas por ano).
        2018+: arquivo consolidado RAIS_VINC_PUB_NORDESTE.7z
        2015-2017: arquivos individuais por UF (AL2015.7z, BA2015.7z, ...)
        """
        if ano_fim is None:
            ano_fim = PERIODO_FIM - 2

        cache = RAW_DIR / "rais" / "nordeste" / "rais_vinculos.csv"
        if cache.exists():
            logger.info("RAIS Nordeste: cache encontrado, pulando coleta.")
            return pd.read_csv(cache)

        vinculos_frames = []
        setor_frames = []

        for ano in range(ano_inicio, ano_fim + 1):
            with tempfile.TemporaryDirectory() as tmpdir:
                arquivos = cls._baixar_rais_ano(ano, tmpdir)
                if not arquivos:
                    logger.warning(f"RAIS {ano}: nenhum arquivo disponível.")
                    continue

                for arq in arquivos:
                    try:
                        df_v, df_s = cls._ler_rais_chunked(str(arq), tmpdir, ano)
                        if not df_v.empty:
                            vinculos_frames.append(df_v)
                        if not df_s.empty:
                            setor_frames.append(df_s)
                    except Exception as e:
                        logger.error(f"RAIS {ano} ({arq.name}): erro ao processar ({e})")
                        continue

        result = None

        if vinculos_frames:
            df_vinculos = pd.concat(vinculos_frames, ignore_index=True)
            # Re-agregar por UF (arquivos individuais geram 1 linha por arquivo)
            df_vinculos = df_vinculos.groupby(["ano", "sigla_uf"], as_index=False).agg(
                vinculos_ativos=("vinculos_ativos", "sum"),
                remuneracao_media=("remuneracao_media", "mean"),
            )
            df_vinculos.sort_values(["ano", "sigla_uf"], inplace=True)
            save_dataframe(df_vinculos, "rais_vinculos", path_parts=["rais", "nordeste"])
            result = df_vinculos
            logger.info(f"RAIS vínculos: {len(df_vinculos)} registros salvos.")

        if setor_frames:
            df_setor = pd.concat(setor_frames, ignore_index=True)
            df_setor = df_setor.groupby(
                ["ano", "sigla_uf", "divisao_cnae"], as_index=False
            ).agg(
                vinculos_ativos=("vinculos_ativos", "sum"),
                remuneracao_media=("remuneracao_media", "mean"),
            )
            df_setor.sort_values(["ano", "sigla_uf", "divisao_cnae"], inplace=True)
            save_dataframe(df_setor, "rais_por_setor", path_parts=["rais", "nordeste"])
            logger.info(f"RAIS por setor: {len(df_setor)} registros salvos.")

        if result is None:
            logger.error("RAIS: nenhum dado coletado.")
        return result

    # =========================================================================
    # Coleta consolidada
    # =========================================================================

    @classmethod
    def coletar_todas(cls) -> int:
        """Coleta todos os datasets CAGED e RAIS do Nordeste via FTP."""
        total = 0

        logger.info("=" * 50)
        logger.info("CAGED Antigo (2015–2019) via FTP/PDET")
        logger.info("=" * 50)
        df = cls.coletar_caged_antigo_nordeste()
        if df is not None:
            total += len(df)

        logger.info("=" * 50)
        logger.info("CAGED — Novo CAGED (2020+) via FTP/PDET")
        logger.info("=" * 50)
        df = cls.coletar_caged_nordeste()
        if df is not None:
            total += len(df)

        logger.info("=" * 50)
        logger.info("RAIS — Vínculos Nordeste via FTP/PDET")
        logger.info("=" * 50)
        df = cls.coletar_rais_nordeste()
        if df is not None:
            total += len(df)

        logger.info(f"CAGED/RAIS total: {total} registros.")
        return total

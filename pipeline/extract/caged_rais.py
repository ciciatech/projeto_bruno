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
        competencia = f"{mes:02d}{str(ano)[2:]}"
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

    @staticmethod
    def _processar_rais_ano(ano: int) -> pd.DataFrame | None:
        """Baixa e lê o arquivo RAIS Nordeste de um ano."""
        remote_path = f"{RAIS_FTP_BASE}/{ano}/RAIS_VINC_PUB_NORDESTE.7z"

        with tempfile.TemporaryDirectory() as tmpdir:
            local_7z = Path(tmpdir) / f"RAIS_VINC_PUB_NORDESTE_{ano}.7z"

            try:
                _ftp_download(remote_path, str(local_7z))
            except error_perm:
                logger.warning(f"RAIS {ano}: arquivo Nordeste não disponível.")
                return None
            except Exception as e:
                logger.warning(f"RAIS {ano}: erro no download ({e})")
                return None

            try:
                df = _read_7z(str(local_7z), tmpdir)
            except Exception as e:
                logger.error(f"RAIS {ano}: erro ao extrair ({e})")
                return None

        # Logar colunas originais
        logger.info(f"RAIS {ano} colunas: {list(df.columns)}")

        # Normalizar colunas RAIS — mapear apenas a primeira ocorrência de cada tipo
        col_map = {}
        mapped_targets = set()
        for col in df.columns:
            lc = col.lower().strip()
            target = None
            if ("município" in lc or "municipio" in lc) and "municipio" not in mapped_targets:
                target = "municipio"
            elif lc == "uf" and "uf" not in mapped_targets:
                target = "uf"
            elif "remun" in lc and ("méd" in lc or "med" in lc) and "nom" in lc and "remuneracao_media" not in mapped_targets:
                target = "remuneracao_media"
            elif "hora" in lc and "contr" in lc and "horas_contratadas" not in mapped_targets:
                target = "horas_contratadas"
            elif "cnae" in lc and ("sub" in lc or "2.0" in lc) and "cnae_subclasse" not in mapped_targets:
                target = "cnae_subclasse"
            elif ("sexo" in lc) and "sexo" not in mapped_targets:
                target = "sexo"
            elif ("raça" in lc or "raca" in lc) and "raca_cor" not in mapped_targets:
                target = "raca_cor"
            elif ("instrução" in lc or "instrucao" in lc or "escolaridade" in lc) and "grau_instrucao" not in mapped_targets:
                target = "grau_instrucao"

            if target:
                col_map[col] = target
                mapped_targets.add(target)

        logger.info(f"RAIS {ano} mapeamento: {col_map}")
        df.rename(columns=col_map, inplace=True)
        df["ano"] = ano

        # Converter UF e adicionar sigla
        if "uf" in df.columns:
            df["uf"] = pd.to_numeric(df["uf"], errors="coerce")
            df["sigla_uf"] = df["uf"].map(UF_IBGE_SIGLA)

        if "remuneracao_media" in df.columns and isinstance(df["remuneracao_media"], pd.Series):
            df["remuneracao_media"] = pd.to_numeric(
                df["remuneracao_media"], errors="coerce"
            )

        logger.info(f"RAIS {ano}: {len(df)} registros.")
        return df

    @classmethod
    def coletar_rais_nordeste(
        cls, ano_inicio: int = PERIODO_INICIO, ano_fim: int = None
    ) -> pd.DataFrame | None:
        """
        Coleta RAIS Nordeste e gera 2 arquivos agregados:
          - rais_vinculos.csv     (UF + ano)
          - rais_por_setor.csv   (UF + ano + seção CNAE)
        """
        if ano_fim is None:
            ano_fim = PERIODO_FIM - 2  # RAIS tem ~2 anos de defasagem

        cache = RAW_DIR / "rais" / "nordeste" / "rais_vinculos.csv"
        if cache.exists():
            logger.info("RAIS Nordeste: cache encontrado, pulando coleta.")
            return pd.read_csv(cache)

        vinculos_frames = []
        setor_frames = []

        for ano in range(ano_inicio, ano_fim + 1):
            df = cls._processar_rais_ano(ano)
            if df is None:
                continue

            # --- Agregação 1: vínculos por UF ---
            if "sigla_uf" in df.columns:
                agg1_dict = {"sigla_uf": ("sigla_uf", "first")}
                agg1_dict["vinculos_ativos"] = ("ano", "count")

                if "remuneracao_media" in df.columns:
                    agg1_dict["remuneracao_media"] = ("remuneracao_media", "mean")
                if "horas_contratadas" in df.columns:
                    agg1_dict["horas_media"] = ("horas_contratadas", "mean")

                agg1 = df.groupby(["ano", "sigla_uf"], as_index=False).agg(
                    vinculos_ativos=("ano", "count"),
                    remuneracao_media=("remuneracao_media", "mean")
                    if "remuneracao_media" in df.columns
                    else ("ano", "count"),
                )
                vinculos_frames.append(agg1)

            # --- Agregação 2: por setor ---
            if "cnae_subclasse" in df.columns and "sigla_uf" in df.columns:
                df["divisao_cnae"] = df["cnae_subclasse"].astype(str).str[:2]
                agg2 = df.groupby(
                    ["ano", "sigla_uf", "divisao_cnae"], as_index=False
                ).agg(
                    vinculos_ativos=("ano", "count"),
                    remuneracao_media=("remuneracao_media", "mean")
                    if "remuneracao_media" in df.columns
                    else ("ano", "count"),
                )
                setor_frames.append(agg2)

        # --- Salvar consolidados ---
        result = None

        if vinculos_frames:
            df_vinculos = pd.concat(vinculos_frames, ignore_index=True)
            df_vinculos.sort_values(["ano", "sigla_uf"], inplace=True)
            save_dataframe(
                df_vinculos, "rais_vinculos", path_parts=["rais", "nordeste"]
            )
            result = df_vinculos
            logger.info(f"RAIS vínculos: {len(df_vinculos)} registros salvos.")

        if setor_frames:
            df_setor = pd.concat(setor_frames, ignore_index=True)
            df_setor.sort_values(["ano", "sigla_uf", "divisao_cnae"], inplace=True)
            save_dataframe(
                df_setor, "rais_por_setor", path_parts=["rais", "nordeste"]
            )
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

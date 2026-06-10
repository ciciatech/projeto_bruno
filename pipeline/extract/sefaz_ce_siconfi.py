"""
Cota-Parte ICMS/IPVA municipal via SICONFI RREO Anexo 03.

Substitui o adapter manual do Ceará Transparente (que está bloqueado por
WAF) — extrai os mesmos dados a partir do Demonstrativo da Receita
Corrente Líquida (RREO Anexo 03) que cada prefeitura cearense já reporta
ao SICONFI.

Estratégia: o RREO Anexo 03 traz a série mensal dos últimos 12 meses
nas colunas <MR> (Mês de Referência), <MR-1>, ..., <MR-11>. Pegamos
o anexo do bimestre 6 (último) que cobre janeiro→dezembro do ano. A query
filtra ``no_anexo=RREO-Anexo 03`` (payload ~4x menor, mesmas linhas).
Volume: 184 munic × 11 anos × 1 bimestre ≈ 2.024 requests.

Checkpoint/resume: o CSV acumulado é regravado a cada município concluído
e, no início da coleta, pares (cod_ibge, ano) já presentes no CSV são
pulados — uma rodada interrompida é retomável com o mesmo comando.

Output:
  dados_nordeste/processed/sefaz_ce_siconfi/transf_estaduais_ce_mensal.csv
  com (cod_ibge, regiao_codigo, regiao_nome, ano, mes, icms, ipva, total)
"""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - fallback mínimo sem dependência
    class tqdm:  # noqa: N801
        """Fallback no-op compatível com tqdm(total=...)/.update()/.close()."""

        def __init__(self, it=None, **kwargs):
            self._it = it

        def __iter__(self):
            return iter(self._it if self._it is not None else [])

        def update(self, n: int = 1):
            pass

        def close(self):
            pass

from pipeline.config import PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL, PROCESSED_DIR
from pipeline.extract.siconfi import Siconfi
from pipeline.regioes_ce import get_regiao_info, get_codigos_municipios_ce
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

ANEXO = "RREO-Anexo 03"
CONTAS = {"Cota-Parte do ICMS": "icms", "Cota-Parte do IPVA": "ipva"}
THROTTLE_S = 0.5
ARQUIVO_SAIDA = "transf_estaduais_ce_mensal"
PATH_PARTS = ["sefaz_ce_siconfi"]
COLUNAS_SAIDA = [
    "cod_ibge", "regiao_codigo", "regiao_nome", "ano", "mes", "icms", "ipva", "total",
]

MR_TO_MES = {
    "<MR>": 12,
    "<MR-1>": 11, "<MR-2>": 10, "<MR-3>": 9, "<MR-4>": 8,
    "<MR-5>": 7, "<MR-6>": 6, "<MR-7>": 5, "<MR-8>": 4,
    "<MR-9>": 3, "<MR-10>": 2, "<MR-11>": 1,
}


def _anos_default() -> list[int]:
    """Anos default da coleta: do início mensal até o último exercício fechado.

    O RREO do bimestre 6 do ano corrente só é publicado após o encerramento
    do exercício — incluir o ano vigente geraria 184 requests inúteis (até
    76s cada). Hoje (config 2015–2026) isto resulta em 2015–2025.
    """
    ano_fim = min(PERIODO_FIM_MENSAL, date.today().year - 1)
    return list(range(PERIODO_INICIO_MENSAL, ano_fim + 1))


def _csv_saida() -> Path:
    """Caminho do CSV acumulado (checkpoint) da coleta."""
    return PROCESSED_DIR.joinpath(*PATH_PARTS) / f"{ARQUIVO_SAIDA}.csv"


def _carregar_acumulado() -> pd.DataFrame:
    """Carrega o CSV existente para resume; DataFrame vazio se não houver."""
    caminho = _csv_saida()
    if not caminho.exists():
        return pd.DataFrame(columns=COLUNAS_SAIDA)
    df = pd.read_csv(
        caminho, encoding="utf-8-sig", dtype={"cod_ibge": str, "regiao_codigo": str},
    )
    df["cod_ibge"] = df["cod_ibge"].str.zfill(7)
    logger.info(f"Resume: CSV existente com {len(df)} linhas em {caminho}")
    return df[COLUNAS_SAIDA]


def _salvar_checkpoint(df: pd.DataFrame) -> pd.DataFrame:
    """Ordena e regrava o CSV acumulado (checkpoint por município)."""
    df = df.sort_values(["ano", "mes", "regiao_codigo", "cod_ibge"]).reset_index(drop=True)
    save_dataframe(df, ARQUIVO_SAIDA, subdir="processed", path_parts=PATH_PARTS)
    return df


def _coletar_um_municipio_ano(cod_ibge: str, ano: int) -> list[dict]:
    raw = Siconfi.coletar_rreo(ano, 6, cod_ibge, "CE", anexo=ANEXO)
    if raw.empty:
        return []
    sub = raw[(raw["anexo"] == ANEXO) & (raw["conta"].isin(CONTAS.keys()))]
    if sub.empty:
        return []

    por_mes: dict[int, dict[str, float]] = {}
    for _, row in sub.iterrows():
        col = row["coluna"]
        if col not in MR_TO_MES:
            continue
        mes = MR_TO_MES[col]
        campo = CONTAS[row["conta"]]
        valor = float(row["valor"])
        por_mes.setdefault(mes, {})[campo] = valor

    return [
        {
            "ano": ano,
            "mes": mes,
            "icms": dados.get("icms", 0.0),
            "ipva": dados.get("ipva", 0.0),
        }
        for mes, dados in por_mes.items()
    ]


def coletar_ce(
    anos: Iterable[int] | None = None,
    municipios: Iterable[str] | None = None,
    throttle: float = THROTTLE_S,
) -> pd.DataFrame:
    """Coleta Cota-Parte ICMS/IPVA mensal (RREO Anexo 03) dos municípios CE.

    Retomável: carrega o CSV acumulado existente, pula pares (cod_ibge, ano)
    já coletados e regrava o CSV a cada município concluído — um crash no
    meio da rodada perde no máximo o município corrente.
    """
    anos = [int(a) for a in anos] if anos else _anos_default()
    municipios = list(municipios) if municipios else get_codigos_municipios_ce()
    municipios = [str(m).zfill(7) for m in municipios]
    info = get_regiao_info().set_index("cod_ibge")[
        ["municipio_nome", "regiao_codigo", "regiao_nome"]
    ]

    acumulado = _carregar_acumulado()
    pares_prontos: set[tuple[str, int]] = (
        set(zip(acumulado["cod_ibge"], acumulado["ano"].astype(int)))
        if not acumulado.empty
        else set()
    )

    total_pares = len(municipios) * len(anos)
    pendentes = sum(
        1 for m in municipios for a in anos if (m, a) not in pares_prontos
    )
    logger.info(
        f"SICONFI Cota-Parte ICMS/IPVA: {len(municipios)} munic x {len(anos)} anos = "
        f"{total_pares} pares ({pendentes} a coletar, "
        f"{total_pares - pendentes} já no CSV — resume)"
    )

    n_munic = len(municipios)
    pbar = tqdm(total=total_pares, desc="SICONFI cota-parte", unit="mun-ano")

    for i, cod_ibge in enumerate(municipios, start=1):
        if cod_ibge not in info.index:
            logger.warning(f"[{i}/{n_munic}] {cod_ibge} fora do mapeamento CE — pulando")
            pbar.update(len(anos))
            continue
        nome = info.loc[cod_ibge, "municipio_nome"]
        regiao_codigo = info.loc[cod_ibge, "regiao_codigo"]
        regiao_nome = info.loc[cod_ibge, "regiao_nome"]

        anos_pendentes = [a for a in anos if (cod_ibge, a) not in pares_prontos]
        if not anos_pendentes:
            logger.info(f"[{i}/{n_munic}] {cod_ibge} {nome} — já no CSV (resume), pulando")
            pbar.update(len(anos))
            continue

        t0 = time.monotonic()
        novas: list[dict] = []
        for ano in anos:
            if ano not in anos_pendentes:
                pbar.update(1)
                continue
            try:
                meses = _coletar_um_municipio_ano(cod_ibge, ano)
            except Exception as e:
                logger.error(f"  {cod_ibge} {ano}: erro — {e}")
                pbar.update(1)
                continue

            for entrada in meses:
                novas.append({
                    "cod_ibge": cod_ibge,
                    "regiao_codigo": regiao_codigo,
                    "regiao_nome": regiao_nome,
                    **entrada,
                    "total": entrada["icms"] + entrada["ipva"],
                })
            time.sleep(throttle)
            pbar.update(1)

        if novas:
            df_novas = pd.DataFrame(novas)[COLUNAS_SAIDA]
            acumulado = (
                df_novas if acumulado.empty
                else pd.concat([acumulado, df_novas], ignore_index=True)
            )
            acumulado = _salvar_checkpoint(acumulado)
        logger.info(
            f"[{i}/{n_munic}] {cod_ibge} {nome} — {len(novas)} linhas, "
            f"{time.monotonic() - t0:.1f} segundos"
        )

    pbar.close()

    if acumulado.empty:
        logger.warning("SICONFI Cota-Parte: nenhum dado coletado")
        return pd.DataFrame()

    logger.info(
        f"SICONFI Cota-Parte: {len(acumulado)} linhas, "
        f"{acumulado['cod_ibge'].nunique()} munic., "
        f"R$ {acumulado['total'].sum() / 1e9:.2f} bi total"
    )
    return acumulado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    coletar_ce()

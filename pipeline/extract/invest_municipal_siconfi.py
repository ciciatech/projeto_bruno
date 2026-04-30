"""
Coletor automatizado de investimento municipal via SICONFI (RREO bimestral).

Substitui o adaptador de planilha manual (``invest_municipal_planilha.py``)
após decisão do Prof. Paulo (abr/2026) de usar SICONFI automático.

Endpoint reaproveitado: ``Siconfi.coletar_rreo(ano, periodo, cod_ibge, uf)``
em ``pipeline/extract/siconfi.py`` — a API aceita ``id_ente`` municipal
(7 dígitos), basta varrer os 184 códigos IBGE do CE.

## Filtro adotado

RREO-Anexo 01 (Balanço Orçamentário), categoria econômica:
- ``cod_conta = "Investimentos"``
- ``coluna   = "DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)"``

O valor é o **empenhado acumulado dentro do ano até o bimestre N**.

## Conversão bimestre → mensal

O fluxo mensal é a diferença entre bimestres consecutivos dividida por 2
(média mensal do bimestre):
- meses 1-2 = valor_b1 / 2
- meses 3-4 = (valor_b2 − valor_b1) / 2
- meses 5-6 = (valor_b3 − valor_b2) / 2
- ... e assim por diante até o bimestre 6 (meses 11-12).

## Heurística anti-erro

Comparar bimestre N+1 com N. Se ``valor_b{n+1} < valor_b{n}`` dentro do
mesmo ano, marca como ``suspeito_relato_corrente`` em auditoria — o
município provavelmente reportou apenas o bimestre corrente em vez do
acumulado. Não descarta o dado: apenas sinaliza para revisão.

Auditoria: ``dados_nordeste/quality/invest_municipal_siconfi_audit.csv``
Saída final: ``dados_nordeste/processed/invest_municipal/invest_municipal_siconfi_ce.csv``
Schema: ``(cod_ibge, regiao_codigo, regiao_nome, ano, mes, valor)``
(idêntico ao adapter da planilha — drop-in replacement).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    def tqdm(iterable, **kwargs):
        return iterable

from pipeline.config import (
    PERIODO_INICIO_MENSAL,
    PERIODO_FIM_MENSAL,
    PROCESSED_DIR,
    QUALITY_DIR,
)
from pipeline.extract.siconfi import Siconfi
from pipeline.regioes_ce import get_regiao_info, get_codigos_municipios_ce
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

ANEXO_INVESTIMENTO = "RREO-Anexo 01"
COD_CONTA_INVESTIMENTO = "Investimentos"
COLUNA_VALOR = "DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)"

THROTTLE_S = 0.5


def _filtrar_investimento(raw: pd.DataFrame) -> float | None:
    """Extrai o valor empenhado-até-bimestre de investimentos do retorno SICONFI."""
    if raw.empty:
        return None
    mask = (
        (raw["anexo"] == ANEXO_INVESTIMENTO)
        & (raw["cod_conta"] == COD_CONTA_INVESTIMENTO)
        & (raw["coluna"] == COLUNA_VALOR)
    )
    sub = raw[mask]
    if sub.empty:
        return None
    return float(sub["valor"].sum())


def _coletar_um_municipio_ano(
    cod_ibge: str, ano: int, throttle: float = THROTTLE_S
) -> dict[int, float | None]:
    """Coleta os 6 bimestres do ano para um município. Retorna {periodo: valor_acumulado}."""
    valores: dict[int, float | None] = {}
    for periodo in range(1, 7):
        raw = Siconfi.coletar_rreo(ano, periodo, cod_ibge, "CE")
        valores[periodo] = _filtrar_investimento(raw)
        time.sleep(throttle)
    return valores


def _bimestres_para_meses(
    valores_acum: dict[int, float | None],
) -> list[tuple[int, float | None]]:
    """
    Converte valores acumulados-até-bimestre em fluxo mensal.

    Retorna lista de (mes, valor) para os 12 meses do ano. Valores ``None``
    permanecem ``None`` (município sem dado naquele bimestre).
    """
    fluxos_b: list[float | None] = []
    anterior = 0.0
    for periodo in range(1, 7):
        atual = valores_acum.get(periodo)
        if atual is None:
            fluxos_b.append(None)
            continue
        fluxo = atual - anterior
        # Anti-relato-corrente é tratado em auditoria; aqui só calculamos.
        fluxos_b.append(max(fluxo, 0.0))  # nunca negativo
        anterior = atual

    meses: list[tuple[int, float | None]] = []
    for i, fluxo_b in enumerate(fluxos_b):
        m1, m2 = 2 * i + 1, 2 * i + 2
        valor_mensal = None if fluxo_b is None else fluxo_b / 2.0
        meses.append((m1, valor_mensal))
        meses.append((m2, valor_mensal))
    return meses


def _detectar_suspeitos(
    valores_acum: dict[int, float | None]
) -> list[tuple[int, str]]:
    """Detecta bimestres com valor acumulado decrescente (relato corrente)."""
    suspeitos: list[tuple[int, str]] = []
    anterior = None
    for periodo in range(1, 7):
        atual = valores_acum.get(periodo)
        if atual is None:
            anterior = atual
            continue
        if anterior is not None and atual < anterior * 0.95:  # tolerância 5%
            suspeitos.append(
                (periodo, f"valor_b{periodo}={atual:.0f} < b{periodo - 1}={anterior:.0f}")
            )
        anterior = atual
    return suspeitos


def coletar_ce(
    anos: Iterable[int] | None = None,
    municipios: Iterable[str] | None = None,
    throttle: float = THROTTLE_S,
) -> pd.DataFrame:
    """
    Coleta investimento municipal via SICONFI RREO para os 184 municípios CE.

    Args:
        anos: range de anos a coletar. Default: PERIODO_INICIO_MENSAL..PERIODO_FIM_MENSAL.
        municipios: subset de cod_ibge. Default: 184 municípios CE.
        throttle: pausa entre requests (segundos).

    Returns:
        DataFrame com (cod_ibge, regiao_codigo, regiao_nome, ano, mes, valor).
    """
    anos = list(anos) if anos else list(range(PERIODO_INICIO_MENSAL, PERIODO_FIM_MENSAL + 1))
    if municipios is None:
        municipios = get_codigos_municipios_ce()
    else:
        municipios = list(municipios)

    info = get_regiao_info().set_index("cod_ibge")[["regiao_codigo", "regiao_nome"]]

    logger.info(
        f"SICONFI invest_municipal: {len(municipios)} municípios × {len(anos)} anos × 6 bimestres "
        f"= {len(municipios) * len(anos) * 6} requests (~{len(municipios) * len(anos) * 6 * throttle / 60:.0f} min)"
    )

    linhas_painel: list[dict] = []
    auditoria: list[dict] = []

    total_iteracoes = len(municipios) * len(anos)
    pbar = tqdm(total=total_iteracoes, desc="SICONFI invest_mun", unit="mun-ano")

    for cod_ibge in municipios:
        cod_ibge = str(cod_ibge).zfill(7)
        if cod_ibge not in info.index:
            logger.warning(f"  município fora do CE: {cod_ibge} — pulando")
            pbar.update(len(anos))
            continue
        regiao_codigo = info.loc[cod_ibge, "regiao_codigo"]
        regiao_nome = info.loc[cod_ibge, "regiao_nome"]

        for ano in anos:
            try:
                valores_acum = _coletar_um_municipio_ano(cod_ibge, ano, throttle)
            except Exception as e:
                logger.error(f"  {cod_ibge} {ano}: erro — {e}")
                pbar.update(1)
                continue

            for periodo, motivo in _detectar_suspeitos(valores_acum):
                auditoria.append(
                    {
                        "cod_ibge": cod_ibge,
                        "ano": ano,
                        "bimestre": periodo,
                        "motivo": "suspeito_relato_corrente",
                        "detalhe": motivo,
                    }
                )

            for mes, valor in _bimestres_para_meses(valores_acum):
                if valor is None:
                    continue
                linhas_painel.append(
                    {
                        "cod_ibge": cod_ibge,
                        "regiao_codigo": regiao_codigo,
                        "regiao_nome": regiao_nome,
                        "ano": ano,
                        "mes": mes,
                        "valor": valor,
                    }
                )
            pbar.update(1)

    pbar.close()

    if not linhas_painel:
        logger.warning("SICONFI invest_municipal: nenhum dado coletado")
        return pd.DataFrame()

    df = pd.DataFrame(linhas_painel).sort_values(
        ["ano", "mes", "regiao_codigo", "cod_ibge"]
    ).reset_index(drop=True)

    save_dataframe(
        df,
        "invest_municipal_siconfi_ce",
        subdir="processed",
        path_parts=["invest_municipal"],
    )

    if auditoria:
        QUALITY_DIR.mkdir(parents=True, exist_ok=True)
        audit_path = QUALITY_DIR / "invest_municipal_siconfi_audit.csv"
        pd.DataFrame(auditoria).to_csv(audit_path, index=False, encoding="utf-8-sig")
        logger.info(
            f"Auditoria: {len(auditoria)} suspeitos salvos em {audit_path} "
            f"({len(auditoria) / max(len(linhas_painel), 1) * 100:.1f}% do total)"
        )

    logger.info(
        f"SICONFI invest_municipal: {len(df)} linhas, "
        f"{df['cod_ibge'].nunique()} municípios, "
        f"R$ {df['valor'].sum() / 1e9:.2f} bi total"
    )
    return df


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Coletor SICONFI invest_municipal CE")
    parser.add_argument("--ano-inicio", type=int, default=PERIODO_INICIO_MENSAL)
    parser.add_argument("--ano-fim", type=int, default=PERIODO_FIM_MENSAL)
    parser.add_argument(
        "--municipios", nargs="*", default=None,
        help="Códigos IBGE 7 dígitos (default: 184 municípios CE)"
    )
    parser.add_argument(
        "--throttle", type=float, default=THROTTLE_S,
        help="Pausa entre requests em segundos"
    )
    args = parser.parse_args()

    coletar_ce(
        anos=range(args.ano_inicio, args.ano_fim + 1),
        municipios=args.municipios,
        throttle=args.throttle,
    )

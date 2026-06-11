"""
Camada bimestral do painel regional CE (T27) + export xlsx no layout do Prof. Paulo.

Agrega ``dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv``
(14 regiões de planejamento × meses) para bimestres (b1=jan-fev ... b6=nov-dez)
segundo a matriz de regras ``pipeline/data/matriz_regras_regional.csv`` — fonte
de verdade da classificação de cada variável, no padrão da
``matriz_regras_modelo.csv`` do pipeline legado:

- **fluxos** (monetários e de movimentação CAGED) → soma dos 2 meses;
- **salario_medio** → média ponderada por ``total_movimentacoes`` (re-mediar
  os 2 meses sem peso introduziria viés — a fonte regional já pondera por
  movimentações em ``preparacao_modelo_regional.py``);
- **estoques** (beneficiários, ``pop``) e **anuais replicados** (``siof_anual_*``,
  ``pop``) → último mês com dado no bimestre (somar valor anual replicado
  dobraria o valor);
- **índices** (``ibcr_ce``, ``share_pib_ce_br``) → média dos 2 meses (decisão
  T27 — diverge do legado ``preparacao_modelo.py``, que usa último valor para
  ibcr/ibc_br; documentado na matriz de regras);
- **nativas bimestrais** (``siof_obras_pago``, ``siof_equip_pago``,
  ``siof_invest_pago`` — fluxo bimestral real do invest. estadual, SIOF
  rel. 544, 2016-2025) → NÃO passam pelo painel mensal: o dado já é bimestral
  na fonte e entra por merge direto (``carregar_siof_bimestral_nativo``);
  documentadas na matriz com ``regra_bimestral = 'nativa_bimestral'``.

A agregação NÃO reusa ``preparacao_modelo.agregar_mensal_para_bimestral``
porque o ``.agg({col: "sum"})`` de lá zera grupos 100% NaN (``min_count=0``) —
aqui 5 regiões não têm CAGED e virariam 0 em vez de NaN. Usamos
``sum(min_count=1)`` para preservar NaN estrutural.

Colunas ``*_real`` (R$ constantes de dez/2025) são adicionadas para TODAS as
colunas monetárias via ``pipeline.transform.deflator.deflator_bimestral`` (T28):
``valor_real = valor_nominal × deflator``, com deflator = I(base)/I(último mês
do bimestre). Bimestres fora da série IPCA (ex.: 2026) ficam com ``*_real`` NaN.

Gate de qualidade (outliers de unidade): antes da agregação, meses cujo total
estadual de uma variável monetária de fluxo excede ``LIMIAR_OUTLIER_MENSAL``×
a mediana da própria série são anulados em TODAS as variáveis da mesma fonte
(assinatura de erro de unidade upstream). O bimestre que contém o mês anulado
vira NaN (somar só o mês restante viraria meio-bimestre disfarçado de valor
válido). Caso histórico: set/2024 das ``transf_fed_*`` (STN) era publicado em
centavos (~100x) e anulava o 24b5; a coleta de jun/2026 corrige a unidade
(÷100) na origem e o gate não flagra mais nenhum mês — segue ativo como
proteção contra regressões upstream. Auditoria (só existe quando há flag):
``dados_nordeste/quality/painel_regional_ce_outliers.csv``.

Ressalvas documentadas no Leia-me do xlsx e na matriz de regras:
- ``invest_mun_valor`` é despesa EMPENHADA acumulada por bimestre (RREO Anexo
  01) — o gabarito do Prof. Paulo usa despesa PAGA (empenhado > pago em ~7% a
  37% a.a.; b1 front-loaded). Migrar para a coluna paga exige re-coleta SICONFI.
- CAGED municipal: recoleta de jun/2026 (bug do dígito verificador corrigido)
  cobre 184/184 municípios e 14/14 regiões, série mensal contínua 2015-01 a
  2026-04 (2025-03 re-baixado após falha transitória de FTP) — ``saldo`` é
  FLUXO (variação) e segue não diretamente comparável ao bloco "Estoque de
  empregos" do gabarito (estoque acumulado).
- Transferências STN: meses 2020-11, 2022-10, 2023-11, 2024-11 e 2025-11 sem
  publicação upstream (``LACUNAS_UPSTREAM_POR_FONTE``) — bimestres 20b6, 22b5,
  23b6, 24b6 e 25b6 anulados nas ``transf_fed_*``; 2018-05 reconstruído por
  inferência item+ordem (validada) e 2021-08 recuperado de arquivo corrigido.
- Crédito BNB (ESTBAN, verbete 160 e aberturas): saldo registrado no município
  da AGÊNCIA (39 dos 184 municípios têm agência BNB) — regiões sem agência
  ficam com 0 por construção; fonte em R$ correntes.

Outputs:
- ``dados_nordeste/processed/model_ready/painel_regional_ce_bimestral.csv``
- ``dados_nordeste/processed/exports/painel_regional_ce_bimestral.xlsx``
  (1 aba "Leia-me" + 1 aba por variável-chave: linhas = bimestre '15b1'...,
  colunas = 14 regiões, valores reais para monetárias)

Uso:
  python3 -m pipeline.transform.painel_bimestral
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from pipeline.config import PROCESSED_DIR, QUALITY_DIR
from pipeline.extract.transferencias_municipais import MESES_SEM_PUBLICACAO_UPSTREAM
from pipeline.regioes_ce import REGIOES_CE
from pipeline.transform.deflator import BASE_PADRAO, deflator_bimestral
from pipeline.utils import save_dataframe

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
MATRIZ_REGRAS_CSV = DATA_DIR / "matriz_regras_regional.csv"

MODEL_READY_DIR = PROCESSED_DIR / "model_ready"
PAINEL_MENSAL_CSV = MODEL_READY_DIR / "painel_regional_ce_mensal.csv"
PAINEL_BIMESTRAL_CSV = MODEL_READY_DIR / "painel_regional_ce_bimestral.csv"
EXPORTS_DIR = PROCESSED_DIR / "exports"
XLSX_PAULO_PATH = EXPORTS_DIR / "painel_regional_ce_bimestral.xlsx"
OUTLIERS_AUDIT_CSV = QUALITY_DIR / "painel_regional_ce_outliers.csv"
SIOF_REGIAO_BIMESTRAL_CSV = (
    PROCESSED_DIR / "execucao_orcamentaria" / "ce" / "siof_regiao_bimestral.csv"
)

#: Valor de ``regra_bimestral`` na matriz para séries NATIVAMENTE bimestrais
#: (não existem no painel mensal — entram por merge direto no bimestral).
REGRA_NATIVA_BIMESTRAL = "nativa_bimestral"
#: Séries de fluxo bimestral real do invest. estadual (SIOF rel. 544, T45).
COLS_SIOF_NATIVO = ["siof_obras_pago", "siof_equip_pago", "siof_invest_pago"]

#: Gate de qualidade — um mês é outlier quando o total estadual da variável
#: excede LIMIAR× a mediana da própria série. Calibração no painel real:
#: o erro de unidade de set/2024 (transf_fed_*) fica em 109x–2783x, enquanto o
#: maior pico legítimo entre variáveis materiais é ~10,9x (invest_fed em dez).
LIMIAR_OUTLIER_MENSAL = 20.0
#: Só variáveis com mediana estadual material entram no gate — séries miúdas e
#: esporádicas (ex.: transf_fed_itr_dest, mediana R$ 67 mil) têm picos
#: legítimos de centenas de x e são anuladas via família da fonte, não
#: diretamente.
PISO_MEDIANA_OUTLIER = 5e7

#: Colunas de identificação do painel mensal (tudo o mais é coluna de valor).
COLS_ID = ["regiao_codigo", "regiao_nome", "ano", "mes"]
#: Chave do painel bimestral.
GRUPO_BIMESTRAL = ["regiao_codigo", "regiao_nome", "ano", "bimestre"]

#: Variáveis-chave exportadas como abas do xlsx (layout do Prof. Paulo).
#: siof_obras_pago / siof_equip_pago / siof_invest_pago são os blocos 4-6 da
#: planilha do Prof. Paulo ("Investimento real ... pago no bimestre pelo
#: estado em equipamentos / em obras / total. Fonte: SIOF") — a variável
#: central da tese, agora com fluxo bimestral REAL nativo do rel. 544.
VARIAVEIS_XLSX = [
    "invest_mun_valor",
    "transf_est_total",
    "saldo",
    "salario_medio",
    "bf_valor_total",
    "bpc_valor_total",
    "invest_fed_total",
    "transf_fed_total",
    "siof_anual_pago",
    "siof_obras_pago",
    "siof_equip_pago",
    "siof_invest_pago",
]

#: Coluna-peso da média ponderada do salário médio.
COL_PESO_SALARIO = "total_movimentacoes"

#: Meses cujo conteúdo NUNCA foi publicado pela fonte, por fonte da matriz de
#: regras. O bimestre que contém um mês não publicado é anulado nas variáveis
#: da fonte — somar só o mês restante viraria meio-bimestre disfarçado de
#: valor válido (mesma política do gate de outliers). Caso documentado: STN
#: publica, em alguns anos, o arquivo AAAAMM com conteúdo de AAAAMM-1 entre
#: set e nov e re-sincroniza em dez — o mês pulado não existe em arquivo algum
#: (ver MESES_SEM_PUBLICACAO_UPSTREAM em extract/transferencias_municipais.py).
LACUNAS_UPSTREAM_POR_FONTE: dict[str, list[tuple[int, int]]] = {
    "transferencias_municipais_stn": MESES_SEM_PUBLICACAO_UPSTREAM,
}


def carregar_matriz_regras() -> pd.DataFrame:
    """Lê a matriz de regras regional (fonte de verdade da agregação T27).

    Colunas: ``variavel, fonte, tipo_temporal, regra_bimestral,
    deflacionamento, unidade_final, observacao``.
    """
    regras = pd.read_csv(MATRIZ_REGRAS_CSV, encoding="utf-8-sig")
    esperadas = {"variavel", "regra_bimestral", "deflacionamento"}
    if not esperadas.issubset(regras.columns):
        raise ValueError(
            f"Matriz de regras inválida ({MATRIZ_REGRAS_CSV}): "
            f"faltam colunas {sorted(esperadas - set(regras.columns))}"
        )
    return regras


def carregar_painel_mensal() -> pd.DataFrame:
    """Lê o painel mensal regional CE model-ready (14 regiões × meses)."""
    if not PAINEL_MENSAL_CSV.exists():
        raise FileNotFoundError(
            f"Painel mensal ausente: {PAINEL_MENSAL_CSV}. "
            "Gere com python3 -m pipeline.transform.preparacao_modelo_regional."
        )
    return pd.read_csv(PAINEL_MENSAL_CSV, dtype={"regiao_codigo": str})


def rotulo_bimestre(ano: int, bimestre: int) -> str:
    """Rótulo de período no estilo da planilha do Prof. Paulo: '15b1' ... '25b6'."""
    return f"{int(ano) % 100:02d}b{int(bimestre)}"


def _validar_cobertura_regras(cols_valor: list[str], regras: pd.DataFrame) -> None:
    """Garante que TODA coluna de valor do painel mensal está classificada na matriz.

    Falha alto em drift de schema (coluna nova sem regra, ou regra órfã).
    Variáveis com ``regra_bimestral == 'nativa_bimestral'`` são documentadas
    na matriz mas NÃO existem no painel mensal (entram por merge direto no
    bimestral — caso SIOF rel. 544); colisão com coluna mensal também falha.
    """
    nativas = set(
        regras.loc[regras["regra_bimestral"] == REGRA_NATIVA_BIMESTRAL, "variavel"]
    )
    classificadas = set(regras["variavel"]) - nativas
    sem_regra = sorted(set(cols_valor) - classificadas)
    orfas = sorted(classificadas - set(cols_valor))
    colisao = sorted(nativas & set(cols_valor))
    if sem_regra or orfas or colisao:
        raise ValueError(
            "Matriz de regras regional fora de sincronia com o painel mensal: "
            f"colunas sem regra={sem_regra}; regras órfãs={orfas}; "
            f"nativas bimestrais colidindo com coluna mensal={colisao}. "
            f"Atualize {MATRIZ_REGRAS_CSV}."
        )


def _salario_medio_ponderado(
    df: pd.DataFrame, col_salario: str, col_peso: str = COL_PESO_SALARIO
) -> pd.Series:
    """Média do salário no bimestre ponderada pelas movimentações de cada mês.

    Meses com salário NaN não entram nem no numerador nem no denominador.
    Retorna Series indexada por ``GRUPO_BIMESTRAL``.
    """
    peso = pd.to_numeric(df[col_peso], errors="coerce").where(df[col_salario].notna())
    tmp = df[GRUPO_BIMESTRAL].copy()
    tmp["_num"] = pd.to_numeric(df[col_salario], errors="coerce") * peso
    tmp["_den"] = peso
    ag = tmp.groupby(GRUPO_BIMESTRAL).sum(min_count=1)
    return ag["_num"] / ag["_den"].where(ag["_den"] > 0)


def carregar_siof_bimestral_nativo() -> pd.DataFrame:
    """Fluxo bimestral NATIVO do invest. estadual (SIOF rel. 544, coleta T45).

    Lê ``siof_regiao_bimestral.csv`` (região × bimestre × elemento, fluxos =
    diff do acumulado entre bimestres) e devolve, por
    ``(regiao_codigo, ano, bimestre)``:

    - ``siof_obras_pago``  — pago no bimestre em obras (elemento 51);
    - ``siof_equip_pago``  — pago no bimestre em equipamentos (elemento 52);
    - ``siof_invest_pago`` — soma 51 + 52.

    NÃO passa pelo painel mensal: o dado já é bimestral na fonte (relatório
    acumulado coletado nos meses pares) — documentado na matriz de regras com
    ``regra_bimestral = 'nativa_bimestral'`` e fonte ``siof_regiao_544``.
    Cobertura 2016-2025 (2015 fica fora: esquema de 8 macrorregiões, códigos
    A*, crosswalk T47); código 15 "Estado do Ceará (não regionalizado)" é
    excluído sem rateio. Fluxos negativos (anulações/estornos) são mantidos.
    """
    vazio = pd.DataFrame(
        {
            "regiao_codigo": pd.Series(dtype=str),
            "ano": pd.Series(dtype=int),
            "bimestre": pd.Series(dtype=int),
            **{c: pd.Series(dtype=float) for c in COLS_SIOF_NATIVO},
        }
    )
    if not SIOF_REGIAO_BIMESTRAL_CSV.exists():
        logger.warning(
            f"SIOF rel. 544 bimestral ausente: {SIOF_REGIAO_BIMESTRAL_CSV} — "
            "colunas siof_*_pago ficarão vazias (rode "
            "python3 -m pipeline.extract.siof_regiao)."
        )
        return vazio
    df = pd.read_csv(
        SIOF_REGIAO_BIMESTRAL_CSV, dtype={"regiao_codigo": str}, encoding="utf-8-sig"
    )
    df = df[df["regiao_codigo"].isin(REGIOES_CE)]
    if df.empty:
        logger.warning("SIOF rel. 544 bimestral: nenhuma linha nas 14 regiões.")
        return vazio

    piv = df.pivot_table(
        index=["regiao_codigo", "ano", "bimestre"],
        columns="elemento", values="pago_fluxo", aggfunc="sum",
    )
    out = pd.DataFrame(
        {
            "siof_obras_pago": piv.get("obras"),
            "siof_equip_pago": piv.get("equip"),
        }
    )
    out["siof_invest_pago"] = out["siof_obras_pago"] + out["siof_equip_pago"]
    out = out.reset_index()
    out["ano"] = out["ano"].astype(int)
    out["bimestre"] = out["bimestre"].astype(int)
    logger.info(
        f"SIOF rel. 544 bimestral nativo: {len(out)} região-bimestre "
        f"({out['ano'].min()}-{out['ano'].max()}, "
        f"{out['regiao_codigo'].nunique()} regiões)"
    )
    return out


def detectar_meses_outlier(
    mensal: pd.DataFrame,
    regras: pd.DataFrame,
    limiar: float = LIMIAR_OUTLIER_MENSAL,
    piso_mediana: float = PISO_MEDIANA_OUTLIER,
) -> pd.DataFrame:
    """Gate de qualidade: detecta meses com total estadual economicamente impossível.

    Varre as variáveis monetárias de fluxo (``regra_bimestral == 'soma'`` e
    ``deflacionamento == 'ipca'``) cuja mediana do total estadual mensal é
    material (``>= piso_mediana``). Um mês é outlier quando o total estadual
    excede ``limiar × mediana`` — assinatura de erro de unidade upstream
    (caso conhecido: set/2024 das ``transf_fed_*``, ~100x os vizinhos).

    Retorna auditoria com colunas ``(ano, mes, variavel, fonte,
    total_estadual, mediana_estadual, razao)``, vazia se nada for flagrado.
    """
    fluxos = regras[
        (regras["regra_bimestral"] == "soma") & (regras["deflacionamento"] == "ipca")
    ]
    achados: list[dict] = []
    for _, linha in fluxos.iterrows():
        var = linha["variavel"]
        if var not in mensal.columns:
            continue
        total = mensal.groupby(["ano", "mes"])[var].sum(min_count=1).dropna()
        total = total[total > 0]
        if total.empty:
            continue
        mediana = float(total.median())
        if mediana < piso_mediana:
            continue
        suspeitos = total[total > limiar * mediana]
        for (ano, mes), valor in suspeitos.items():
            achados.append(
                {
                    "ano": int(ano),
                    "mes": int(mes),
                    "variavel": var,
                    "fonte": linha["fonte"],
                    "total_estadual": float(valor),
                    "mediana_estadual": mediana,
                    "razao": float(valor) / mediana,
                }
            )
    cols = [
        "ano", "mes", "variavel", "fonte",
        "total_estadual", "mediana_estadual", "razao",
    ]
    if not achados:
        return pd.DataFrame(columns=cols)
    return (
        pd.DataFrame(achados)[cols]
        .sort_values(["ano", "mes", "variavel"])
        .reset_index(drop=True)
    )


def aplicar_gate_outliers(
    mensal: pd.DataFrame,
    regras: pd.DataFrame,
    limiar: float = LIMIAR_OUTLIER_MENSAL,
    piso_mediana: float = PISO_MEDIANA_OUTLIER,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Anula (NaN) os meses-outlier em TODAS as variáveis da mesma fonte.

    Erro de unidade upstream contamina a fonte inteira no mês (ex.: set/2024
    afeta fpm, fundeb, itr, outros, royalties e total da STN), então a anulação
    é por família — inclusive as séries miúdas que escapam do piso de
    materialidade do detector.

    Retorna ``(mensal_limpo, auditoria)``; o painel original não é modificado.
    """
    auditoria = detectar_meses_outlier(mensal, regras, limiar, piso_mediana)
    if auditoria.empty:
        return mensal, auditoria

    limpo = mensal.copy()
    fonte_por_var = dict(zip(regras["variavel"], regras["fonte"]))
    for ano, mes, fonte in (
        auditoria[["ano", "mes", "fonte"]].drop_duplicates().itertuples(index=False)
    ):
        vars_fonte = [
            v for v, f in fonte_por_var.items() if f == fonte and v in limpo.columns
        ]
        mask = (limpo["ano"] == ano) & (limpo["mes"] == mes)
        limpo.loc[mask, vars_fonte] = float("nan")
        logger.warning(
            f"Gate de outliers: {ano}-{mes:02d} anulado para a fonte '{fonte}' "
            f"({len(vars_fonte)} variáveis: {vars_fonte}) — total estadual "
            f">{limiar:.0f}x a mediana (provável erro de unidade upstream)."
        )
    return limpo, auditoria


def _anular_bimestres_afetados(
    painel: pd.DataFrame, auditoria: pd.DataFrame, regras: pd.DataFrame
) -> pd.DataFrame:
    """Anula no bimestral as células dos bimestres que contêm um mês-outlier.

    Sem isto, a soma ``min_count=1`` usaria apenas o mês restante e o bimestre
    viraria meio-período disfarçado de valor válido (ex.: 24b5 só com outubro).
    """
    if auditoria.empty:
        return painel
    fonte_por_var = dict(zip(regras["variavel"], regras["fonte"]))
    for ano, mes, fonte in (
        auditoria[["ano", "mes", "fonte"]].drop_duplicates().itertuples(index=False)
    ):
        bimestre = (int(mes) + 1) // 2
        vars_fonte = [
            v for v, f in fonte_por_var.items() if f == fonte and v in painel.columns
        ]
        mask = (painel["ano"] == ano) & (painel["bimestre"] == bimestre)
        painel.loc[mask, vars_fonte] = float("nan")
    return painel


def agregar_para_bimestre(mensal: pd.DataFrame, regras: pd.DataFrame) -> pd.DataFrame:
    """Agrega o painel mensal para (regiao_codigo, regiao_nome, ano, bimestre).

    A regra de cada coluna vem da matriz (``soma`` / ``media`` / ``ultimo_mes`` /
    ``media_ponderada_movimentacoes``). Somas usam ``min_count=1`` para que
    grupos 100% NaN permaneçam NaN (não virem 0).
    """
    df = mensal.copy()
    df["ano"] = df["ano"].astype(int)
    df["mes"] = df["mes"].astype(int)
    df["bimestre"] = (df["mes"] + 1) // 2
    df = df.sort_values(["regiao_codigo", "ano", "mes"]).reset_index(drop=True)

    cols_valor = [c for c in mensal.columns if c not in COLS_ID]
    _validar_cobertura_regras(cols_valor, regras)

    regra_por_var = dict(zip(regras["variavel"], regras["regra_bimestral"]))
    cols_soma = [c for c in cols_valor if regra_por_var[c] == "soma"]
    cols_media = [c for c in cols_valor if regra_por_var[c] == "media"]
    cols_ultimo = [c for c in cols_valor if regra_por_var[c] == "ultimo_mes"]
    cols_ponderadas = [
        c for c in cols_valor if regra_por_var[c] == "media_ponderada_movimentacoes"
    ]
    desconhecidas = sorted(
        set(cols_valor) - set(cols_soma + cols_media + cols_ultimo + cols_ponderadas)
    )
    if desconhecidas:
        raise ValueError(
            f"Regras bimestrais desconhecidas para {desconhecidas} "
            f"(verifique a coluna regra_bimestral em {MATRIZ_REGRAS_CSV})."
        )

    g = df.groupby(GRUPO_BIMESTRAL, sort=True)
    partes: list[pd.DataFrame] = []
    if cols_soma:
        partes.append(g[cols_soma].sum(min_count=1))
    if cols_media:
        partes.append(g[cols_media].mean())
    if cols_ultimo:
        # último mês com dado no bimestre (df já ordenado por ano, mes)
        partes.append(g[cols_ultimo].last())
    out = pd.concat(partes, axis=1)

    for col in cols_ponderadas:
        out[col] = _salario_medio_ponderado(df, col)

    out = out.reset_index()[GRUPO_BIMESTRAL + cols_valor]
    return (
        out.sort_values(["ano", "bimestre", "regiao_codigo"]).reset_index(drop=True)
    )


def construir_painel_bimestral(
    base: str = BASE_PADRAO, painel_mensal: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Constrói o painel bimestral regional CE com colunas reais (R$ ``base``).

    Lê o painel mensal (ou usa ``painel_mensal`` injetado — útil em testes),
    aplica o gate de outliers (meses com erro de unidade upstream viram NaN na
    família da fonte — ver ``aplicar_gate_outliers``), agrega para bimestre
    segundo a matriz de regras (bimestres que contêm mês anulado ficam NaN) e
    adiciona ``deflator`` (IPCA, base pinada — padrão dez/2025) e ``<col>_real``
    para toda coluna monetária (``deflacionamento == 'ipca'`` na matriz).

    Retorna DataFrame com chave ``(regiao_codigo, regiao_nome, ano, bimestre)``.
    """
    regras = carregar_matriz_regras()
    mensal = painel_mensal if painel_mensal is not None else carregar_painel_mensal()

    mensal_limpo, outliers = aplicar_gate_outliers(mensal, regras)
    if not outliers.empty:
        QUALITY_DIR.mkdir(parents=True, exist_ok=True)
        outliers.to_csv(OUTLIERS_AUDIT_CSV, index=False, encoding="utf-8-sig")
        logger.warning(
            f"Gate de outliers: {len(outliers)} flag(s) em "
            f"{outliers[['ano', 'mes']].drop_duplicates().shape[0]} mês(es) — "
            f"auditoria salva em {OUTLIERS_AUDIT_CSV}"
        )
    else:
        # Sem flags nesta execução: remove auditoria de execuções anteriores
        # (um CSV obsoleto sugeriria outliers que não existem mais nos dados).
        if OUTLIERS_AUDIT_CSV.exists():
            OUTLIERS_AUDIT_CSV.unlink()
            logger.info(
                f"Gate de outliers: nenhum mês flagrado — auditoria obsoleta "
                f"removida ({OUTLIERS_AUDIT_CSV})"
            )
    painel = agregar_para_bimestre(mensal_limpo, regras)
    painel = _anular_bimestres_afetados(painel, outliers, regras)

    # Lacunas de publicação upstream: mesmo tratamento dos meses-outlier — o
    # bimestre meio-coberto vira NaN na família da fonte, não meio-valor.
    lacunas = pd.DataFrame(
        [
            {"ano": ano, "mes": mes, "fonte": fonte}
            for fonte, meses in LACUNAS_UPSTREAM_POR_FONTE.items()
            for ano, mes in meses
        ]
    )
    if not lacunas.empty:
        painel = _anular_bimestres_afetados(painel, lacunas, regras)
        logger.info(
            f"Lacunas upstream: {len(lacunas)} mês(es) sem publicação na fonte "
            f"({sorted(lacunas['fonte'].unique())}) — bimestres anulados."
        )

    # Séries NATIVAMENTE bimestrais (SIOF rel. 544): merge direto — não passam
    # pelo mensal nem pelo gate de outliers (a coleta já valida contra o spike).
    siof_nativo = carregar_siof_bimestral_nativo()
    painel = painel.merge(
        siof_nativo, on=["regiao_codigo", "ano", "bimestre"], how="left"
    )

    defl = deflator_bimestral(base=base)
    painel = painel.merge(defl, on=["ano", "bimestre"], how="left")

    cols_monetarias = regras.loc[
        regras["deflacionamento"] == "ipca", "variavel"
    ].tolist()
    for col in cols_monetarias:
        painel[f"{col}_real"] = painel[col] * painel["deflator"]

    logger.info(
        f"Painel bimestral regional CE: {len(painel)} linhas "
        f"({painel['regiao_codigo'].nunique()} regiões × "
        f"{painel.groupby(['ano', 'bimestre']).ngroups} bimestres), "
        f"{len(painel.columns)} colunas ({len(cols_monetarias)} *_real, base {base})"
    )
    return painel


def _trim_extremos_vazios(tabela: pd.DataFrame) -> pd.DataFrame:
    """Remove apenas as linhas 100% vazias das EXTREMIDADES da tabela.

    Bordas vazias refletem a cobertura real da fonte (ex.: BPC só a partir de
    2019, SIOF só 2026). Linhas vazias no MEIO da série são mantidas — marcam
    período anulado pelo gate de outliers (ex. histórico: 24b5 das
    ``transf_fed_*`` antes da correção de set/2024) e não podem sumir
    silenciosamente do entregável.
    """
    cheias = tabela.notna().any(axis=1)
    if not cheias.any():
        return tabela.iloc[0:0]
    posicoes = cheias.to_numpy().nonzero()[0]
    return tabela.iloc[posicoes[0] : posicoes[-1] + 1]


def _coluna_para_aba(
    painel: pd.DataFrame, variavel: str, monetarias: set[str]
) -> tuple[str, str]:
    """Decide a coluna (real vs nominal) usada na aba de ``variavel`` do xlsx.

    Monetárias usam ``<var>_real`` (R$ base dez/2025); se a coluna real estiver
    100% vazia (ex.: siof_anual_pago, só 2026 — sem IPCA do ano), cai para o
    nominal com a unidade documentada no Leia-me.
    """
    col_real = f"{variavel}_real"
    if variavel in monetarias and col_real in painel.columns:
        if painel[col_real].notna().any():
            return col_real, "real (R$ dez/2025)"
        return variavel, "nominal (R$ correntes — sem IPCA p/ deflacionar o período)"
    return variavel, "nominal (variável não monetária)"


def exportar_xlsx_paulo(
    painel: pd.DataFrame | None = None, path: Path | str | None = None
) -> Path:
    """Exporta o painel bimestral no layout do Prof. Paulo (xlsx).

    1 aba "Leia-me" (o que é, regras de agregação, base monetária dez/2025 e
    fonte por variável) + 1 aba por variável-chave (``VARIAVEIS_XLSX``):
    linhas = rótulo de bimestre '15b1'...; colunas = as 14 regiões de
    planejamento (grafia de ``REGIOES_CE``); valores reais para monetárias.
    Linhas 100% vazias nas extremidades são omitidas (cobertura real da
    fonte); vazios no meio da série são mantidos visíveis (gate de outliers).

    Retorna o ``Path`` do arquivo gerado.
    """
    if painel is None:
        painel = construir_painel_bimestral()
    out_path = Path(path) if path is not None else XLSX_PAULO_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)

    regras = carregar_matriz_regras()
    monetarias = set(regras.loc[regras["deflacionamento"] == "ipca", "variavel"])

    df = painel.copy()
    df["periodo"] = [
        rotulo_bimestre(a, b) for a, b in zip(df["ano"], df["bimestre"])
    ]
    ordem_periodos = (
        df.sort_values(["ano", "bimestre"])["periodo"].drop_duplicates().tolist()
    )
    ordem_regioes = list(REGIOES_CE.values())

    # Decidir real vs nominal por aba ANTES de escrever (vai para o Leia-me)
    abas: list[tuple[str, str, str]] = []  # (variavel, coluna_usada, descricao)
    for var in VARIAVEIS_XLSX:
        col, descricao = _coluna_para_aba(df, var, monetarias)
        abas.append((var, col, descricao))
    valores_por_var = {var: descricao for var, _, descricao in abas}

    intro = [
        "Painel bimestral regional do Ceará — 14 regiões de planejamento (SEPLAG/IPECE)",
        "",
        "O que é: agregação bimestral (b1=jan-fev, b2=mar-abr, ..., b6=nov-dez) do painel mensal",
        "model-ready painel_regional_ce_mensal.csv (fontes públicas: CAGED, Bolsa Família, BPC,",
        "STN, SEFAZ-CE/SICONFI, SIOF-CE, Portal da Transparência, BACEN, IBGE, IPEA).",
        "",
        "Regras de agregação mensal → bimestre (matriz pipeline/data/matriz_regras_regional.csv):",
        "  - fluxos (valores pagos/transferidos, movimentações CAGED): SOMA dos 2 meses;",
        "  - salário médio: MÉDIA PONDERADA por total_movimentacoes de cada mês;",
        "  - estoques (beneficiários, população) e valores anuais replicados (siof_anual_*):",
        "    ÚLTIMO mês com dado no bimestre (somar valor anual replicado dobraria o valor);",
        "  - índices (IBCR-CE, share PIB CE/BR): MÉDIA dos 2 meses.",
        "",
        "Investimento estadual SIOF (relatório 544, região × elemento 51/52):",
        "  - siof_obras_pago / siof_equip_pago / siof_invest_pago (= 51 + 52): FLUXO bimestral",
        "    REAL pago pelo estado em obras / equipamentos por região, 2016-2025 — dado",
        "    NATIVAMENTE bimestral (diff do acumulado do relatório entre bimestres; não passa",
        "    pelo painel mensal). Fluxos negativos = anulações/estornos da fonte (mantidos).",
        "    Correspondem aos blocos 'Investimento real ... pago no bimestre pelo estado em",
        "    equipamentos / em obras / total — Fonte: SIOF' da planilha de referência.",
        "  - siof_anual_* : fechamento ANUAL replicado nos 12 meses — 2016-2025 do rel. 544 +",
        "    2026 da extração corrente; split anual em siof_obras_pago_anual/siof_equip_pago_anual.",
        "  - 2015 fica vazio (esquema antigo de 8 macrorregiões — crosswalk 14→8 pendente, T47);",
        "    a despesa NÃO regionalizada (cód. 15 'Estado do Ceará') é excluída sem rateio —",
        "    desprezível em obras (0,4-2,3%/ano), até 24,8% (2019) em equipamentos.",
        "",
        f"Base monetária: valores reais em R$ constantes de dez/2025 (IPCA, SGS 433 BACEN).",
        "Deflator = I(dez/2025) / I(último mês do bimestre), com I = índice IPCA acumulado",
        "fim-de-mês — mesma convenção da aba 'Índice de inflação' da planilha de referência.",
        "",
        "Gate de qualidade (outliers de unidade): meses cujo total estadual de uma variável",
        f"monetária excede {LIMIAR_OUTLIER_MENSAL:.0f}x a mediana da própria série são ANULADOS em todas as",
        "variáveis da mesma fonte (assinatura de erro de unidade na origem). Caso histórico:",
        "set/2024 das transferências federais STN era publicado em CENTAVOS pela fonte (~100x)",
        "e anulava a linha 24b5 da aba transf_fed_total; a coleta de jun/2026 corrige a",
        "unidade (divisão por 100) e o 24b5 voltou a ser válido — nesta versão NENHUM mês é",
        "anulado pelo gate, que segue ativo como proteção. Auditoria (gerada só quando há",
        "flag): dados_nordeste/quality/painel_regional_ce_outliers.csv.",
        "",
        "ATENÇÃO — estágio da despesa em invest_mun_valor: são despesas EMPENHADAS acumuladas",
        "até o bimestre (RREO Anexo 01, col. 'DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)'), NÃO",
        "despesas PAGAS como no bloco 'Investimento real... pago no bimestre' da planilha de",
        "referência. Empenhado supera o pago em ~7% a 37% ao ano, com b1 front-loaded (2-3x)",
        "e b6 abaixo (0,6-0,75x). Migrar para a coluna de despesas pagas do RREO exige",
        "re-coleta SICONFI (pendência documentada na matriz de regras).",
        "",
        "CAGED municipal: recoleta de jun/2026 (bug do dígito verificador na consulta",
        "corrigido) cobre os 184 dos 184 municípios do CE e as 14 regiões de planejamento,",
        "com série mensal CONTÍNUA de 2015-01 a 2026-04 (136 meses — o mês 2025-03, perdido",
        "por falha transitória do FTP do MTE na primeira recoleta, foi re-baixado; um guard",
        "de continuidade no coletor passa a acusar qualquer mês interior faltante).",
        "Atenção conceitual: 'saldo' é FLUXO (admissões - desligamentos no bimestre) e NÃO é",
        "diretamente comparável ao bloco 'Estoque de empregos' da planilha de referência",
        "(estoque acumulado de vínculos).",
        "",
        "Transferências federais STN (transf_fed_*): a fonte (CKAN/Tesouro) NÃO publicou o",
        "conteúdo dos meses 2020-11, 2022-10, 2023-11, 2024-11 e 2025-11 — nesses anos o",
        "arquivo nomeado AAAAMM traz conteúdo de AAAAMM-1 entre set e nov e re-sincroniza",
        "em dezembro, pulando um mês (evidência: o adicional de 1% do FPM de setembro,",
        "EC 84/2014, aparece no mês rotulado setembro). Os bimestres 20b6, 22b5, 23b6, 24b6",
        "e 25b6 ficam VAZIOS nas transf_fed_* (somar só o mês restante viraria meio-bimestre",
        "disfarçado de valor válido). O mês 2018-05 (arquivo publicado sem a coluna",
        "'Transferência') foi reconstruído por inferência determinística item + ordem de",
        "ocorrência, validada com zero divergências nos arquivos bem formados de 2018-2021;",
        "2021-08 foi recuperado do arquivo corrigido pela fonte.",
        "",
        "Crédito BNB (ESTBAN/BACEN, verbete 160 e aberturas bnb_*): saldo (estoque) de",
        "operações de crédito registrado no município da AGÊNCIA — 39 dos 184 municípios do",
        "CE têm agência BNB; regiões sem agência ficam com 0 por construção. Agregação por",
        "ÚLTIMO mês do bimestre (estoque — somar dobraria); fonte em R$ correntes,",
        "deflacionada para R$ dez/2025 nas colunas *_real.",
        "",
        "Cada aba traz uma variável: linhas = bimestre ('15b1' = jan-fev/2015, ...);",
        "colunas = as 14 regiões de planejamento. Abas monetárias em valores REAIS;",
        "exceções documentadas na coluna 'valores_na_aba' da tabela abaixo. Linhas sem",
        "valor real nas extremidades são omitidas (ex.: 2026 nas abas siof_* — sem IPCA",
        "do ano ainda não há valor real; o nominal segue no CSV model-ready).",
        "",
        "Fonte, regra e unidade por variável:",
    ]

    regras_doc = regras.copy()
    regras_doc["aba_no_xlsx"] = regras_doc["variavel"].isin(VARIAVEIS_XLSX).map(
        {True: "sim", False: "nao"}
    )
    regras_doc["valores_na_aba"] = regras_doc["variavel"].map(valores_por_var).fillna("")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        regras_doc.to_excel(
            writer, sheet_name="Leia-me", startrow=len(intro) + 1, index=False
        )
        ws = writer.sheets["Leia-me"]
        for i, linha in enumerate(intro, start=1):
            ws.cell(row=i, column=1, value=linha)

        for var, col, _ in abas:
            tabela = _trim_extremos_vazios(
                df.pivot(index="periodo", columns="regiao_nome", values=col)
                .reindex(index=ordem_periodos, columns=ordem_regioes)
            )
            tabela.index.name = "periodo"
            tabela.to_excel(writer, sheet_name=var)

    logger.info(f"Export xlsx (layout Prof. Paulo): {out_path} ({1 + len(abas)} abas)")
    return out_path


def main() -> None:
    """Constrói o painel bimestral, salva o CSV model-ready e exporta o xlsx."""
    painel = construir_painel_bimestral()
    save_dataframe(
        painel,
        "painel_regional_ce_bimestral",
        subdir="processed",
        path_parts=["model_ready"],
    )
    xlsx = exportar_xlsx_paulo(painel)
    print(
        f"\n=== PAINEL BIMESTRAL REGIONAL CE (T27) ===\n"
        f"  CSV : {PAINEL_BIMESTRAL_CSV} ({len(painel)} linhas × {len(painel.columns)} colunas)\n"
        f"  XLSX: {xlsx} (Leia-me + {len(VARIAVEIS_XLSX)} variáveis)\n"
        f"  Bimestres: {rotulo_bimestre(painel['ano'].min(), 1)} a "
        f"{rotulo_bimestre(painel['ano'].max(), 6)} | base monetária {BASE_PADRAO}"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    main()

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
  ibcr/ibc_br; documentado na matriz de regras).

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
(assinatura de erro de unidade upstream). Caso conhecido: set/2024 das
``transf_fed_*`` (STN), ~100x os meses vizinhos — sem o gate, a célula 24b5
ficava ~20x os vizinhos no xlsx entregue ao Prof. Paulo. O bimestre que contém
o mês anulado vira NaN (somar só o mês restante viraria meio-bimestre
disfarçado de valor válido). Auditoria:
``dados_nordeste/quality/painel_regional_ce_outliers.csv``.

Ressalvas documentadas no Leia-me do xlsx e na matriz de regras:
- ``invest_mun_valor`` é despesa EMPENHADA acumulada por bimestre (RREO Anexo
  01) — o gabarito do Prof. Paulo usa despesa PAGA (empenhado > pago em ~7% a
  37% a.a.; b1 front-loaded). Migrar para a coluna paga exige re-coleta SICONFI.
- CAGED municipal cobre só 18 dos 184 municípios: 5 regiões sem nenhum
  município e as outras 9 PARCIAIS — ``saldo``/``salario_medio`` regionais NÃO
  são comparáveis ao bloco "Estoque de empregos" do gabarito.

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
]

#: Coluna-peso da média ponderada do salário médio.
COL_PESO_SALARIO = "total_movimentacoes"


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
    """Garante que TODA coluna de valor do painel está classificada na matriz.

    Falha alto em drift de schema (coluna nova sem regra, ou regra órfã).
    """
    classificadas = set(regras["variavel"])
    sem_regra = sorted(set(cols_valor) - classificadas)
    orfas = sorted(classificadas - set(cols_valor))
    if sem_regra or orfas:
        raise ValueError(
            "Matriz de regras regional fora de sincronia com o painel mensal: "
            f"colunas sem regra={sem_regra}; regras órfãs={orfas}. "
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
    painel = agregar_para_bimestre(mensal_limpo, regras)
    painel = _anular_bimestres_afetados(painel, outliers, regras)

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
    período anulado pelo gate de outliers (ex.: 24b5 das ``transf_fed_*``) e
    não podem sumir silenciosamente do entregável.
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
        f"Base monetária: valores reais em R$ constantes de dez/2025 (IPCA, SGS 433 BACEN).",
        "Deflator = I(dez/2025) / I(último mês do bimestre), com I = índice IPCA acumulado",
        "fim-de-mês — mesma convenção da aba 'Índice de inflação' da planilha de referência.",
        "",
        "Gate de qualidade (outliers de unidade): meses cujo total estadual de uma variável",
        f"monetária excede {LIMIAR_OUTLIER_MENSAL:.0f}x a mediana da própria série são ANULADOS em todas as",
        "variáveis da mesma fonte (assinatura de erro de unidade na origem). Caso conhecido:",
        "set/2024 das transferências federais STN (~100x; ex.: FPM de Fortaleza R$ 12 bi vs",
        "R$ 75 mi nos meses vizinhos) — por isso a linha 24b5 da aba transf_fed_total está",
        "vazia (o bimestre inteiro é anulado para não virar meio-período). Auditoria:",
        "dados_nordeste/quality/painel_regional_ce_outliers.csv.",
        "",
        "ATENÇÃO — estágio da despesa em invest_mun_valor: são despesas EMPENHADAS acumuladas",
        "até o bimestre (RREO Anexo 01, col. 'DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)'), NÃO",
        "despesas PAGAS como no bloco 'Investimento real... pago no bimestre' da planilha de",
        "referência. Empenhado supera o pago em ~7% a 37% ao ano, com b1 front-loaded (2-3x)",
        "e b6 abaixo (0,6-0,75x). Migrar para a coluna de despesas pagas do RREO exige",
        "re-coleta SICONFI (pendência documentada na matriz de regras).",
        "",
        "ATENÇÃO — cobertura PARCIAL do CAGED municipal: a fonte processada cobre só 18 dos",
        "184 municípios do CE. Além das 5 regiões sem nenhum município (Cariri; Litoral Leste;",
        "Litoral Oeste / Vale do Curu; Serra da Ibiapaba; Sertão dos Inhamuns), as outras 9 são",
        "parciais (Grande Fortaleza 5/19; Maciço do Baturité 4/13; Centro Sul 2/13; Sertão",
        "Central 2/13; Litoral Norte 1/13; Sertão de Canindé 1/6; Sertão de Sobral 1/18;",
        "Sertão dos Crateús 1/13; Vale do Jaguaribe 1/15). As abas 'saldo' e 'salario_medio'",
        "refletem APENAS esses municípios — o saldo regional fica na ordem de 6% da variação",
        "de estoque do gabarito (mediana) e NÃO é comparável ao bloco 'Estoque de empregos'",
        "da planilha de referência.",
        "",
        "Cada aba traz uma variável: linhas = bimestre ('15b1' = jan-fev/2015, ...);",
        "colunas = as 14 regiões de planejamento. Abas monetárias em valores REAIS;",
        "exceções documentadas na coluna 'valores_na_aba' da tabela abaixo",
        "(ex.: siof_anual_pago em R$ correntes — dado só de 2026, ainda sem IPCA do ano).",
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

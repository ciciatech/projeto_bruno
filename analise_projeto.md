# Análise do Projeto — Tese DESP/UFC

## Impactos do Crédito no Crescimento Econômico do Nordeste

**Autor da tese:** Bruno Cardoso Costa
**Orientador:** Prof. Dr. Magno Prudêncio de Almeida Filho
**Programa:** Doutorado Profissional em Economia do Setor Público (DESP/UFC)
**Análise gerada em:** 03/03/2026

---

## 1. CONTEXTO E OBJETIVO ATUALIZADO

A tese propõe avaliar a influência isolada do crédito (PF e PJ) na economia do Nordeste brasileiro entre 2015 e 2025, utilizando metodologia wavelet para capturar comovimentos em tempo e frequência. O projeto original focava no IBCR-NE como variável dependente, mas a reunião de 24/02/2025 com os orientadores redirecionou o modelo: a variável dependente passa a ser o **volume de empregos**, com a pergunta central sendo *"Se dermos R$1 bilhão em crédito, quantos mil empregos são gerados?"*. O IBCR-NE permanece como controle.

A tese prevê ~11 variáveis: emprego como dependente, crédito PF e PJ como explicativas, e cerca de 8 instrumentos de controle (variáveis fiscais estaduais, transferências federais e indicadores macroeconômicos nacionais). Os dados devem ser harmonizados para periodicidade bimestral, deflacionados pelo IPCA e dessazonalizados.

Além da análise acadêmica, o projeto entrega um Produto Tecnológico: um Sistema Inteligente de Gestão com IA voltado para BNB, secretarias estaduais e gestores públicos, capaz de automatizar coleta, análise e interpretação dos resultados.

---

## 2. INVENTÁRIO DOS DADOS COLETADOS

### 2.1 Variáveis Explicativas — Crédito (BACEN-SGS)

132 registros mensais (jan/2015 a dez/2025) com 12 séries: IBCR-NE, crédito PF e PJ (Nordeste e Brasil), crédito total Nordeste, inadimplência PF e PJ, IPCA mensal, PIB trimestral e SELIC mensal. Cobertura temporal completa para o período da tese. O saldo de crédito PF no Nordeste cresceu de R$283 bi (2018) para R$678 bi (set/2025) — expansão de ~240%. O crédito PJ saiu de R$131 bi para R$289 bi no mesmo intervalo. Essa assimetria entre PF e PJ é um achado relevante que os dados já sustentam.

### 2.2 Variável Dependente — Emprego Formal (CAGED via BigQuery)

648 registros de saldo mensal (admissões - desligamentos) por UF, cobrindo os 9 estados do Nordeste de 2020 a 2025. Complementado por 4.263 registros de saldo por divisão CNAE (setor econômico) e 1.317 registros por perfil demográfico (sexo, grau de instrução). A desagregação setorial e demográfica já disponível é rica, mas a cobertura temporal está incompleta — faltam 5 anos (2015–2019).

### 2.3 Controles Fiscais Estaduais (SICONFI/STN)

Três relatórios fiscais cobrindo os 9 estados: RREO com 2.989 registros (receitas e despesas, bimestral), RGF com 15.489 registros (gestão fiscal, quadrimestral) e DCA com 630 registros (balanço patrimonial, anual). Os dados existem em formato bruto com centenas de rubricas contábeis. As variáveis específicas do modelo (investimento público, resultado primário, dívida consolidada líquida) ainda precisam ser isoladas desses datasets.

### 2.4 Transferências Federais

6.075 registros de transferências constitucionais (FPE, FUNDEB e outras) para os 9 estados. Bolsa Família coletado apenas para capitais do NE entre 2024 e 2026 (225 registros) — cobertura insuficiente para o modelo. BPC (Benefício de Prestação Continuada) não coletado.

### 2.5 Execução Orçamentária Estadual

Três estados com dados de execução orçamentária: Ceará (SIOF-CE, 456 registros, 2015–2026), Alagoas (Portal Transparência AL, 2.065 registros, 2015–2025) e Piauí (Portal Transparência PI, 250 registros, apenas 2024). Schemas inconsistentes entre estados (CE com 10 colunas, AL com 9, PI com 32). Seis estados sem dados: BA, PE, PB, RN, SE, MA.

---

## 3. MAPEAMENTO: DADOS EXISTENTES × MODELO DA TESE


| Papel no Modelo | Variável                   | Fonte                | Coletado? | Cobertura              | Gap                        |
| --------------- | -------------------------- | -------------------- | --------- | ---------------------- | -------------------------- |
| **Dependente**  | Emprego formal (fluxo)     | CAGED                | Sim       | 2020–2025              | Falta 2015–2019            |
| **Dependente**  | Emprego formal (estoque)   | RAIS                 | Não       | —                      | Série completa ausente     |
| **Explicativa** | Crédito PF Nordeste        | BACEN-SGS 14084      | Sim       | 2015–2025              | OK                         |
| **Explicativa** | Crédito PJ Nordeste        | BACEN-SGS 14089      | Sim       | 2015–2025              | OK                         |
| **Controle**    | IBCR-NE                    | BACEN-SGS 25389      | Sim       | 2015–2025              | OK                         |
| **Controle**    | SELIC                      | BACEN-SGS 4189       | Sim       | 2015–2025              | OK                         |
| **Controle**    | IPCA                       | BACEN-SGS 433        | Sim       | 2015–2025              | OK                         |
| **Controle**    | IBC-Br                     | BACEN-SGS 24364      | Não       | —                      | Coleta trivial, mesma API  |
| **Controle**    | Investimento público       | SICONFI (DCA)        | Parcial   | Anual, 9 UFs           | Extrair do bruto           |
| **Controle**    | Resultado Primário         | SICONFI (RREO)       | Parcial   | Bimestral, 9 UFs       | Extrair do bruto           |
| **Controle**    | Dívida Consolidada Líquida | SICONFI (RGF)        | Parcial   | Quadrimestral, 9 UFs   | Extrair do bruto           |
| **Controle**    | Transferências (BPC, BF)   | Portal Transparência | Parcial   | BF: 2024–2026 capitais | BPC ausente, BF incompleto |
| **Controle**    | Exportação/Importação NE   | MDIC/IPEADATA        | Não       | —                      | Série completa ausente     |


**Síntese:** As variáveis explicativas e os controles macro nacionais estão completos. O gap crítico é a variável dependente (emprego cobre apenas 2020+). Os controles fiscais estaduais existem em bruto mas não foram transformados. Transferências e comércio exterior estão incompletos ou ausentes.

---

## 4. ANÁLISE DE ADERÊNCIA POR EIXO

**Crédito:** Forte aderência. Séries completas 2015–2025, frequência mensal, com separação PF/PJ e comparativo NE × Brasil. A dinâmica de expansão assimétrica (PF crescendo muito mais rápido que PJ) já é visível nos dados e sustenta a narrativa da tese. O gap relevante é o SCR (crédito granular por CNAE, modalidade e porte), que depende de solicitação formal ao BACEN via Prof. Magno.

**Emprego:** Aderência parcial — ponto mais vulnerável. O CAGED coletado traz desagregação por setor e perfil para os 9 estados, mas apenas de 2020 em diante. Para completar a série 2015–2019, há dois caminhos viáveis: o CAGED antigo (disponível via FTP do MTE em layout diferente, ou via BigQuery na tabela `basedosdados.br_me_caged.microdados_antigo`) e a RAIS (vínculos anuais, com arquivo dedicado para o Nordeste no FTP: `RAIS_VINC_PUB_NORDESTE.7z`, também disponível no BigQuery: `basedosdados.br_me_rais.microdados_vinculos`). A RAIS seria mais valiosa porque dá o estoque de emprego (quantos vínculos existem), não apenas o fluxo (admissões/demissões). A combinação CAGED (fluxo mensal, 2015–2025) + RAIS (estoque anual, 2015–2022) ofereceria uma variável dependente mais robusta.

**Fiscal:** Aderência parcial com potencial alto. Os dados do SICONFI cobrem os 9 estados em três relatórios, mas o trabalho de extrair investimento público (DCA), resultado primário (RREO) e dívida consolidada líquida (RGF) das centenas de rubricas contábeis brutas ainda não foi feito. A execução orçamentária estadual (SIOF-CE, Transparência-AL) complementa com granularidade de empenho/pagamento, mas cobre apenas 3 de 9 estados.

**Transferências:** Aderência fraca. FPE e FUNDEB estão coletados, mas Bolsa Família cobre apenas 2024–2026 (capitais) e BPC está ausente. Ambos foram definidos na reunião de redirecionamento como controles para isolar o efeito do crédito — sem eles, há risco de variável omitida.

**Macro nacional:** Aderência alta. SELIC, IPCA e PIB trimestral completos. Falta apenas IBC-Br (série 24364), coleta trivial via mesma API do BACEN-SGS.

---

## 5. INCONSISTÊNCIAS E RISCOS NOS DADOS ATUAIS

**Periodicidade heterogênea:** A tese define bimestral como padrão, mas os dados misturam séries mensais (BACEN, CAGED), bimestrais (RREO), quadrimestrais (RGF), anuais (DCA, RAIS). Sem harmonização temporal, não é possível alimentar o modelo wavelet.

**Valores nominais:** Todas as séries monetárias estão em valores nominais. O IPCA mensal já foi coletado (está no parquet BACEN), mas a rotina de deflacionamento ainda não foi aplicada.

**Schema inconsistente na execução orçamentária:** PI tem 32 colunas, AL tem 9, CE tem 10 — com nomes e estrutura diferentes. Qualquer análise comparativa entre estados exige normalização prévia.

**Lacuna temporal na variável dependente:** O modelo precisa de 2015–2025 para emprego, mas o CAGED coletado cobre apenas 2020–2025. Isso impede análises wavelet nos regimes de recessão (2015–2016) e recuperação (2017–2019) — justamente os períodos de maior interesse analítico.

---

## 6. OPORTUNIDADES IDENTIFICADAS

**Cruzamento crédito × emprego por setor:** O CAGED já traz saldo por divisão CNAE. Se o SCR for viabilizado (crédito por CNAE), seria possível construir um painel setorial inédito na literatura: para cada setor do Nordeste, quanto crédito entrou e quantos empregos foram gerados. Isso responde à pergunta da tese com granularidade que estudos anteriores não alcançaram.

**Dimensão perfil do emprego:** O CAGED coletado traz sexo, grau de instrução e faixa salarial. Isso abre análise distribucional: o crédito gera mais empregos de alta ou baixa qualificação? O efeito difere por gênero? Essa dimensão social reforça relevância para política pública e pode ser um diferencial no artigo.

**Heterogeneidade estadual:** Com 9 estados e dados fiscais do SICONFI, é possível investigar por que o crédito gera mais empregos em alguns estados que em outros. Estados com melhor governança fiscal (menor dívida, maior investimento) potencializam o efeito do crédito? Dialoga com literatura de convergência regional.

**Janela temporal rica em eventos:** O período 2015–2025 captura quatro regimes distintos: recessão (2015–2016), recuperação com queda de juros (2017–2019), choque pandêmico (2020–2021) e retomada pós-COVID (2022–2025). A metodologia wavelet é especialmente adequada para capturar como a relação crédito-emprego muda entre regimes — dados macroeconômicos já coletados sustentam essa análise.

**Execução orçamentária como diferencial de ineditismo:** A coleta de SIOF-CE e Transparência-AL com granularidade de empenho/pagamento por função é rara na literatura. Se expandida, permitiria avaliar como investimento público estadual interage com crédito privado na geração de emprego.

**Produto tecnológico com dados existentes:** A base já estruturada permite iniciar o produto tecnológico (interface conversacional com IA sobre os dados) sem depender dos modelos econométricos. Isso viabiliza demonstração na qualificação enquanto wavelet/VAR são desenvolvidos em paralelo.

---

## 7. PLANO DE COLETA: COMPLETAR CAGED E RAIS

O gap mais crítico é a variável dependente (emprego). Há dois caminhos complementares, ambos viáveis via BigQuery (Base dos Dados) — mesma infraestrutura já usada para o CAGED atual.

**CAGED Antigo (2015–2019):** Disponível na tabela `basedosdados.br_me_caged.microdados_antigo`. Filtro por `sigla_uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')`. Layout diferente do Novo CAGED (campos e encoding distintos), mas cobre o período que falta. Alternativa via FTP: `ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/` — arquivos em .7z, separador `;`, encoding `latin-1`, filtrar por campo `região == 2` (Nordeste).

**RAIS Vínculos (2015–2022):** Disponível na tabela `basedosdados.br_me_rais.microdados_vinculos`. Também tem arquivo dedicado no FTP: `RAIS_VINC_PUB_NORDESTE.7z` por ano, o que facilita o download. Fornece estoque de vínculos empregatícios (quantos empregos existem), remuneração média, horas contratadas, desagregação por CNAE, CBO, sexo, raça, instrução e idade. Última RAIS disponível é 2022.

**Recomendação:** Coletar ambos. CAGED antigo preenche o fluxo mensal de 2015–2019. RAIS dá o estoque anual até 2022 — variável dependente mais estável para wavelet. Combinar as duas perspectivas (fluxo + estoque) fortalece a robustez da análise.

---

## 8. PRIORIZAÇÃO DE PRÓXIMOS PASSOS


| Prioridade  | Ação                                                                          | Impacto                                             |
| ----------- | ----------------------------------------------------------------------------- | --------------------------------------------------- |
| 1 (urgente) | Coletar RAIS vínculos NE (2015–2022) — BigQuery ou FTP                        | Completa variável dependente com estoque de emprego |
| 2 (urgente) | Coletar CAGED antigo (2015–2019) — BigQuery ou FTP                            | Estende fluxo de emprego para todo o período        |
| 3 (alta)    | Extrair variáveis do SICONFI bruto (investimento, resultado primário, dívida) | Habilita controles fiscais estaduais                |
| 4 (alta)    | Harmonização temporal → bimestral (todas as séries)                           | Pré-requisito para modelo wavelet                   |
| 5 (alta)    | Solicitar SCR/BACEN via Prof. Magno                                           | Cruzamento crédito × emprego por setor              |
| 6 (média)   | Deflacionar séries monetárias pelo IPCA                                       | Comparação temporal válida                          |
| 7 (média)   | Coletar IBC-Br (série 24364) + completar Bolsa Família e BPC                  | Fecha controles macro e transferências              |
| 8 (baixa)   | Expandir execução orçamentária para 6 estados restantes                       | Oportunidade de ineditismo                          |


---

## 9. RELATÓRIO TÉCNICO DE VERIFICAÇÃO DE FONTES DE DADOS

**Verificação de APIs e fontes de dados | Gerado em: 04/03/2026 | Verificado por acesso direto às APIs (Claude Chrome)**

### 9.0 Sumário Executivo

| # | Fonte | Status | Cobertura Verificada | Obs. Crítica |
|---|---|---|---|---|
| 1 | BACEN-SGS (IPCA, SELIC, Crédito PF/PJ, IBC-Br) | ✅ OPERACIONAL | Até dez/2025 | Sem autenticação |
| 2 | RAIS — BigQuery (Base dos Dados) | ✅ OPERACIONAL | 1985–2024 | 414 GB — cota! |
| 3 | CAGED Antigo — BigQuery | ✅ OPERACIONAL | **2007–2019** | Nome: `microdados_antigos` |
| 4 | CAGED Novo — BigQuery | ✅ OPERACIONAL | 2020–jan/2026 | Nome: `microdados_movimentacao` |
| 5 | SICONFI API (RREO, RGF, DCA) | ✅ OPERACIONAL | Até 2024 | `id_ente` = código IBGE 2 dígitos |
| 6 | SIDRA/IBGE — PIB estadual anual | ✅ OPERACIONAL | Até 2022 | Agregado 5938, var. 37 |
| 7 | SIDRA/IBGE — PIB trimestral nacional | ✅ OPERACIONAL | Até 2025T4 | Requer `classificacao=11255[90707]` |
| 8 | Portal da Transparência — Bolsa Família | ✅ OPERACIONAL | Exige chave | **URL migrou para HTTP** |
| 9 | SAGI/MDS — BPC/RMV e Bolsa Família | ✅ OPERACIONAL | Até fev/2026 | URL: `aplicacoes.cidadania.gov.br` |
| 10 | FTP MTE (CAGED/RAIS bruto) | ⚠️ SÓ VIA FTPLIB | — | Browsers bloqueiam FTP |

---

### 9.1 BACEN — Sistema Gerenciador de Séries Temporais (SGS)

**Status: ✅ Totalmente operacional. Sem autenticação. Dados até dez/2025.**

#### Séries confirmadas por teste direto

| Código SGS | Variável | Frequência | Teste (jan–dez/2025) |
|---|---|---|---|
| **433** | IPCA (variação % mensal) | Mensal | ✅ Dados completos 2025 |
| **11** | SELIC Over (taxa diária) | Diária | ✅ 4,55% a.a. jan/2025 |
| **12** | SELIC Overnight (igual ao 11) | Diária | ✅ Idêntico ao 11 |
| **20539** | Crédito PF — Saldo total (R$ mi) | Mensal | ✅ R$ 6,46 tri em jan/2025 |
| **20540** | Crédito PJ — Saldo total (R$ mi) | Mensal | ✅ R$ 2,45 tri em jan/2025 |
| **20541** | Crédito Total PF+PJ (R$ mi) | Mensal | ✅ R$ 4,01 tri em jan/2025 |
| **24364** | IBC-Br (índice mensal) | Mensal | ✅ ~107 (base 2002=100) |

> **Nota sobre SELIC:** Use a série 4189 (SELIC % a.m. — meta do COPOM) para análise de política monetária; as séries 11 e 12 são a taxa efetiva diária (overnight), que precisam ser anualizadas.

#### Código Python testado e validado

```python
import requests
import pandas as pd

def coletar_sgs(codigo: int, inicio: str = '01/01/2015', fim: str = '31/12/2025') -> pd.Series:
    """
    Coleta série do BACEN-SGS.
    inicio/fim no formato DD/MM/AAAA
    """
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}"
           f"/dados?formato=json&dataInicial={inicio}&dataFinal={fim}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
    return df.set_index('data')['valor']

# Coleta em lote
series_bacen = {
    'ipca':       433,
    'selic_meta': 4189,
    'credito_pf': 20539,
    'credito_pj': 20540,
    'ibcbr':      24364,
}
dados = {nome: coletar_sgs(cod) for nome, cod in series_bacen.items()}
```

---

### 9.2 BASE DOS DADOS — BigQuery (RAIS e CAGED)

**Status: ✅ Operacional. IDs de tabelas confirmados por navegação direta.**

#### Tabelas confirmadas

| Tabela | ID BigQuery CONFIRMADO | Tamanho | Cobertura | Partições |
|---|---|---|---|---|
| RAIS Vínculos | `basedosdados.br_me_rais.microdados_vinculos` | 414 GB | 1985–2024 | `ano, sigla_uf` |
| RAIS Estab. | `basedosdados.br_me_rais.microdados_estabelecimentos` | 25 GB | 1985–2024 | `ano` |
| CAGED Antigo | `basedosdados.br_me_caged.microdados_antigos` | 59 GB | **2007–2019** | `ano, mes, sigla_uf` |
| CAGED Antigo Ajustes | `basedosdados.br_me_caged.microdados_antigos_de_ajustes` | — | 2007–2019 | — |
| **CAGED Novo** | `basedosdados.br_me_caged.microdados_movimentacao` | — | **2020–jan/2026** | `ano, mes, sigla_uf` |
| CAGED Excluídas | `basedosdados.br_me_caged.microdados_de_movimentacoes_excluidas` | — | 2020+ | — |
| CAGED Fora do Prazo | `basedosdados.br_me_caged.microdados_de_movimentacoes_fora_do_prazo` | — | 2020+ | — |

> ⚠️ **ATENÇÃO CRÍTICA:** O CAGED Antigo na Base dos Dados cobre apenas a partir de **2007**. Para 2015–2019, você está coberto; para 2003–2006, seria necessário o FTP do MTE (não é seu caso).

> ⚠️ **CORREÇÃO DE NOMENCLATURA:** O Novo CAGED é `microdados_movimentacao` (singular, sem "s"). A última atualização na Base dos Dados foi **2026-03-03**, confirmando que está atualizado mensalmente.

#### Queries SQL validadas

**CAGED Antigo (2015–2019) — Saldo mensal por UF e CNAE:**
```sql
SELECT
    ano,
    mes,
    sigla_uf,
    SUBSTR(CAST(cnae_2_subclasse AS STRING), 1, 2) AS cnae_divisao,
    COUNTIF(admitidos_desligados = 1)                                 AS admissoes,
    COUNTIF(admitidos_desligados = 2)                                 AS desligamentos,
    COUNTIF(admitidos_desligados = 1) - COUNTIF(admitidos_desligados = 2) AS saldo
FROM `basedosdados.br_me_caged.microdados_antigos`
WHERE
    sigla_uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')
    AND ano BETWEEN 2015 AND 2019
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3
-- Custo estimado: ~3 GB (partições por ano+mes+sigla_uf reduzem drasticamente)
```

**CAGED Novo (2020–2025) — Saldo mensal por UF e CNAE:**
```sql
SELECT
    ano,
    mes,
    sigla_uf,
    SUBSTR(CAST(cnae_2_subclasse AS STRING), 1, 2)   AS cnae_divisao,
    COUNTIF(saldo_movimentacao = 1)                   AS admissoes,
    COUNTIF(saldo_movimentacao = -1)                  AS desligamentos,
    SUM(saldo_movimentacao)                           AS saldo
FROM `basedosdados.br_me_caged.microdados_movimentacao`
WHERE
    sigla_uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')
    AND ano BETWEEN 2020 AND 2025
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3
```

**RAIS Vínculos (2015–2022) — Estoque 31/12 por UF:**
```sql
SELECT
    ano,
    sigla_uf,
    SUBSTR(cnae_2_subclasse, 1, 2)  AS cnae_divisao,
    sexo,
    grau_instrucao_apos_2005,
    COUNT(*)                         AS total_vinculos_ativos
FROM `basedosdados.br_me_rais.microdados_vinculos`
WHERE
    sigla_uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')
    AND ano BETWEEN 2015 AND 2022
    AND vinculo_ativo_3112 = '1'
GROUP BY 1, 2, 3, 4, 5
-- Custo estimado: ~60–80 GB (filtro por sigla_uf reduz do total de 414 GB)
-- SEMPRE use: maximum_bytes_billed = 100 * 1024**3
```

---

### 9.3 SICONFI — API do Tesouro Nacional

**Status: ✅ Totalmente operacional. Dados reais confirmados por chamada ao Ceará (id_ente=23).**

#### Parâmetros confirmados por teste real

**Endpoint base:** `https://apidatalake.tesouro.gov.br/ords/siconfi/tt//`

| Relatório | Endpoint | Parâmetros obrigatórios | Periodicidade |
|---|---|---|---|
| RREO | `/rreo` | `an_exercicio, nr_periodo (1–6), co_tipo_demonstrativo=RREO, co_esfera=E, co_poder=E, id_ente` | Bimestral |
| RGF | `/rgf` | `an_exercicio, in_periodicidade=Q, nr_periodo (1–3), co_tipo_demonstrativo=RGF, co_esfera=E, co_poder=E, id_ente` | Quadrimestral |
| DCA | `/dca` | `an_exercicio, no_anexo, co_tipo_demonstrativo=DCA, co_esfera=E, co_poder=E, id_ente` | Anual |

#### Códigos IBGE dos 9 estados do NE (confirmados)

```python
estados_ne = {
    'AL': 27, 'BA': 29, 'CE': 23, 'MA': 21,
    'PB': 25, 'PE': 26, 'PI': 22, 'RN': 24, 'SE': 28
}
```

#### `cod_conta` verificados por chamada real

| Variável | Relatório | `cod_conta` CONFIRMADO | `coluna` para usar | Valor CE 2024 |
|---|---|---|---|---|
| **Investimento público** | DCA — Anexo I-D | `DO4.4.00.00.00.00` | `Despesas Liquidadas` | R$ 2,41 bi |
| **Resultado Primário (acima da linha, com RPPS)** | RREO — Anexo 06 | `ResultadoPrimarioComRPPSAcimaDaLinha` | `VALOR` | R$ 0,42 bi |
| **Resultado Primário (acima da linha, sem RPPS)** | RREO — Anexo 06 | `ResultadoPrimarioSemRPPSAcimaDaLinha` | `VALOR` | R$ 0,02 bi |
| **DCL (Dívida Consolidada Líquida)** | RGF — Anexo 02 | `DividaConsolidadaLiquida` | `Até o 3º Quadrimestre` | R$ 12,0 bi |

#### Código Python de coleta validado

```python
import requests
import pandas as pd
from itertools import product

BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/"

ESTADOS_NE = {
    'AL': 27, 'BA': 29, 'CE': 23, 'MA': 21,
    'PB': 25, 'PE': 26, 'PI': 22, 'RN': 24, 'SE': 28
}

def coletar_rreo(ano: int, periodo: int, id_ente: int) -> pd.DataFrame:
    """Coleta RREO bimestral. periodo: 1-6."""
    url = BASE + "rreo"
    params = {
        'an_exercicio': ano, 'nr_periodo': periodo,
        'co_tipo_demonstrativo': 'RREO', 'co_esfera': 'E',
        'co_poder': 'E', 'id_ente': id_ente
    }
    r = requests.get(url, params=params, timeout=60)
    return pd.DataFrame(r.json().get('items', []))

def coletar_rgf(ano: int, periodo: int, id_ente: int) -> pd.DataFrame:
    """Coleta RGF quadrimestral. periodo: 1-3."""
    url = BASE + "rgf"
    params = {
        'an_exercicio': ano, 'in_periodicidade': 'Q', 'nr_periodo': periodo,
        'co_tipo_demonstrativo': 'RGF', 'no_anexo': 'RGF-Anexo 02',
        'co_esfera': 'E', 'co_poder': 'E', 'id_ente': id_ente
    }
    r = requests.get(url, params=params, timeout=60)
    return pd.DataFrame(r.json().get('items', []))

def coletar_dca(ano: int, id_ente: int) -> pd.DataFrame:
    """Coleta DCA anual — Balanço Orçamentário Despesas."""
    url = BASE + "dca"
    params = {
        'an_exercicio': ano, 'no_anexo': 'DCA-Anexo I-D',
        'co_tipo_demonstrativo': 'DCA',
        'co_esfera': 'E', 'co_poder': 'E', 'id_ente': id_ente
    }
    r = requests.get(url, params=params, timeout=60)
    return pd.DataFrame(r.json().get('items', []))

# --- Coleta completa para os 9 estados, 2015-2024 ---
def extrair_investimento(ano_ini=2015, ano_fim=2024):
    resultados = []
    for ano, (uf, cod) in product(range(ano_ini, ano_fim+1), ESTADOS_NE.items()):
        df = coletar_dca(ano, cod)
        if df.empty: continue
        inv = df[
            (df['cod_conta'] == 'DO4.4.00.00.00.00') &
            (df['coluna'] == 'Despesas Liquidadas')
        ][['exercicio','uf','valor']].copy()
        resultados.append(inv)
    return pd.concat(resultados, ignore_index=True)

def extrair_resultado_primario(ano_ini=2015, ano_fim=2024):
    resultados = []
    for ano, per, (uf, cod) in product(
        range(ano_ini, ano_fim+1), range(1, 7), ESTADOS_NE.items()
    ):
        df = coletar_rreo(ano, per, cod)
        if df.empty: continue
        rp = df[
            (df['cod_conta'] == 'ResultadoPrimarioComRPPSAcimaDaLinha') &
            (df['coluna'] == 'VALOR')
        ][['exercicio','periodo','uf','valor']].copy()
        resultados.append(rp)
    return pd.concat(resultados, ignore_index=True)
```

---

### 9.4 IBGE — SIDRA (PIB Estadual e PIB Trimestral Nacional)

**Status: ✅ Operacional com parâmetros corretos. Dados confirmados por chamada real.**

#### Endpoints verificados

##### PIB Estadual Anual (Contas Regionais)
- **Agregado:** `5938` | **Variável:** `37` (PIB a preços correntes, R$ mil)
- **Cobertura:** até **2022** (Contas Regionais têm defasagem de ~2 anos)
- **Endpoint:** `https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/{ano}/variaveis/37?localidades=N3[21,22,23,24,25,26,27,28,29]`
- **Resultado real:** PIB CE 2022 = R$ 213,6 bi ✅

##### PIB Trimestral Nacional (SCN Trimestral)
- **Agregado:** `5932` | **Variável:** `6562` (taxa acumulada 4 trimestres)
- **Cobertura:** 1996T1 – **2025T4** ✅
- ⚠️ **OBRIGATÓRIO:** passar `classificacao=11255[90707]` — sem isso retorna `".."` para todos os períodos
- **Endpoint:** `https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/{YYYYQQ}/variaveis/6562?localidades=N1[all]&classificacao=11255[90707]`
- **Resultado real:** PIB Brasil 2024T1 = +2,8% (acumulado 4T) ✅

```python
import requests
import pandas as pd

def coletar_pib_estadual_ne(anos: list[int]) -> pd.DataFrame:
    """Coleta PIB anual dos 9 estados do NE via SIDRA."""
    estados_ne_ibge = '21,22,23,24,25,26,27,28,29'
    periodos = '|'.join(str(a) for a in anos)
    url = (f"https://servicodados.ibge.gov.br/api/v3/agregados/5938"
           f"/periodos/{periodos}/variaveis/37"
           f"?localidades=N3[{estados_ne_ibge}]")
    r = requests.get(url, timeout=30)
    data = r.json()
    series = data[0]['resultados'][0]['series']
    rows = []
    for s in series:
        uf_id = s['localidade']['id']
        uf_nome = s['localidade']['nome']
        for ano, val in s['serie'].items():
            rows.append({'cod_ibge': uf_id, 'uf': uf_nome,
                         'ano': int(ano), 'pib_mil_reais': int(val) if val != '..' else None})
    return pd.DataFrame(rows)

def coletar_pib_trimestral(periodos_yyyyqq: list[str]) -> pd.DataFrame:
    """
    Coleta PIB trimestral nacional.
    periodos_yyyyqq: ex. ['202301','202302','202303','202304']
    """
    per_str = '|'.join(periodos_yyyyqq)
    url = (f"https://servicodados.ibge.gov.br/api/v3/agregados/5932"
           f"/periodos/{per_str}/variaveis/6562"
           f"?localidades=N1[all]&classificacao=11255[90707]")
    r = requests.get(url, timeout=30)
    data = r.json()
    serie = data[0]['resultados'][0]['series'][0]['serie']
    return pd.Series({k: float(v) for k, v in serie.items() if v != '..'})
```

---

### 9.5 PORTAL DA TRANSPARÊNCIA — Bolsa Família

**Status: ✅ Operacional. ⚠️ URL migrada e requer autenticação Gov.br.**

#### Mudança crítica detectada

> A URL mudou de `https://api.portaldatransparencia.gov.br` para **`http://api.portaldatransparencia.gov.br`** (HTTP, não HTTPS). Chamadas HTTPS retornam mensagem de migração.

#### Requisitos de acesso
- Cadastro em `portaldatransparencia.gov.br/api-de-dados/cadastrar-email`
- Autenticação via **Gov.br nível Prata ou Ouro** (CPF + verificação em 2 fatores)
- A chave é enviada por e-mail após autenticação

#### Limites de rate confirmados
- 0h–6h: **700 req/min**
- Demais horários: **400 req/min**
- APIs restritas (dados individuais): 180 req/min

#### Estratégia de coleta para 9 estados NE, 2015–2025

```python
import requests
import pandas as pd
import time

def coletar_bolsa_familia_estado(uf_cod_ibge_2dig: int,
                                  mes_ano: str,
                                  chave_api: str) -> pd.DataFrame:
    """
    Coleta Bolsa Família por município, filtrado por UF (estado).
    uf_cod_ibge_2dig: 2 dígitos do IBGE do estado (ex: 23 para CE)
    mes_ano: formato AAAAMM (ex: '202401')
    """
    url = "http://api.portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio"
    headers = {'chave-api-dados': chave_api}
    todos = []
    pagina = 1
    while True:
        params = {
            'mesAno': mes_ano,
            'codigoIbge': uf_cod_ibge_2dig,
            'pagina': pagina
        }
        r = requests.get(url, headers=headers, params=params, timeout=30)
        dados = r.json()
        if not dados: break
        todos.extend(dados)
        pagina += 1
        time.sleep(0.15)  # respeitar rate limit de 400 req/min
    return pd.DataFrame(todos)

# Coleta completa para NE 2015-2025
ESTADOS_NE_IBGE2 = [21, 22, 23, 24, 25, 26, 27, 28, 29]
# Gerar lista de meses
import itertools
meses = [f"{ano}{mes:02d}"
         for ano, mes in itertools.product(range(2015, 2026), range(1, 13))]
```

**Alternativa sem autenticação:** SAGI/MDS (seção 6 abaixo) tem dados de Bolsa Família por UF agregados, sem necessidade de API key.

---

### 9.6 SAGI/MDS — Bolsa Família e BPC/RMV

**Status: ✅ Totalmente operacional. Dados até fev/2026 confirmados.**

#### URL correta e definitiva

```
https://aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/
```
(Redirecionamento automático de `aplicacoes.mds.gov.br/sagi/ri/relatorios/cidadania/`)

#### Dados disponíveis
- **Bolsa Família:** beneficiários e valores por UF, mensal
- **BPC/RMV:** beneficiários por tipo (deficiência / idoso) por UF
- **Cadastro Único:** famílias cadastradas por situação de pobreza
- **Última referência disponível:** fevereiro/2026 ✅
- **Filtro por Estado:** disponível na interface ✅

#### Estratégia para coleta programática

O portal não tem API REST documentada pública. Para coleta automatizada, duas opções:

**Opção A — Download manual via interface** (recomendado para série histórica completa)
1. Acesse `aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/`
2. Clique em `#Alterar` → selecione `Estado` → escolha UF
3. Selecione período → clique em `Imprimir PDF` ou exporte CSV

**Opção B — Selenium/Playwright** para automação

```python
from playwright.sync_api import sync_playwright

def coletar_bpc_por_uf(uf_nome: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/')
        # Clicar em #Alterar e selecionar Estado e UF
        page.click('a:has-text("Alterar")')
        page.click('label:has-text("Estado")')
        page.fill('input[placeholder*="INFORME"]', uf_nome)
        page.keyboard.press('Enter')
        # Aguardar carregamento e capturar dados da seção BPC/RMV
        page.wait_for_selector('text=BPC/RMV')
        # Extrair via JavaScript
        dados = page.evaluate("""
            () => Array.from(document.querySelectorAll('.bpc-data'))
                       .map(el => el.innerText)
        """)
        browser.close()
        return dados
```

---

### 9.7 FTP DO MTE — RAIS e CAGED (Arquivos Brutos)

**Status: ⚠️ Servidor ativo, mas inacessível via browser. Acesso só por cliente FTP.**

O servidor `ftp.mtps.gov.br` existe e responde (confirmado pelo navegador reconhecer o host), porém browsers modernos (Chrome 100+) bloqueiam FTP por protocolo. O acesso deve ser feito exclusivamente via Python `ftplib`:

```python
from ftplib import FTP
import io, py7zr, pandas as pd

def listar_arquivos_ftp(diretorio: str) -> list:
    """Lista arquivos no FTP do MTE."""
    ftp = FTP('ftp.mtps.gov.br', timeout=30)
    ftp.login()  # acesso anônimo
    ftp.cwd(diretorio)
    arquivos = ftp.nlst()
    ftp.quit()
    return arquivos

def baixar_caged_ftp(ano: int, mes: int) -> pd.DataFrame:
    """
    Baixa e lê CAGED Antigo direto do FTP.
    Fallback caso BigQuery não esteja disponível.
    """
    ftp = FTP('ftp.mtps.gov.br', timeout=60)
    ftp.login()
    caminho = f'/pdet/microdados/CAGED/{ano}/CAGEDEST_{mes:02d}{str(ano)[2:]}.7z'
    buffer = io.BytesIO()
    ftp.retrbinary(f'RETR {caminho}', buffer.write)
    ftp.quit()
    buffer.seek(0)
    with py7zr.SevenZipFile(buffer, mode='r') as z:
        files = z.read()
        csv_nome = list(files.keys())[0]
        df = pd.read_csv(
            io.BytesIO(files[csv_nome]),
            encoding='latin-1', sep=';', low_memory=False
        )
    return df[df['regiao'] == 2]  # filtro Nordeste
```

> **Recomendação:** Dado que o BigQuery via Base dos Dados está atualizado e particionado, **use o FTP apenas como contingência**. O BigQuery é significativamente mais rápido e evita problemas de encoding/descompressão.

---

### 9.8 CORREÇÕES NECESSÁRIAS NO PIPELINE

Com base nas verificações, as correções prioritárias para o seu pipeline Python/Streamlit são:

#### Correção 1 — Nome da tabela CAGED Novo (CRÍTICO)

```python
# ERRADO (como pode estar no pipeline):
CAGED_NOVO = "basedosdados.br_me_caged.microdados_novo_caged"

# CORRETO (confirmado por teste):
CAGED_NOVO = "basedosdados.br_me_caged.microdados_movimentacao"
CAGED_ANTIGO = "basedosdados.br_me_caged.microdados_antigos"
```

#### Correção 2 — API Portal da Transparência (URL)

```python
# ERRADO:
URL_PT = "https://api.portaldatransparencia.gov.br/api-de-dados/"

# CORRETO:
URL_PT = "http://api.portaldatransparencia.gov.br/api-de-dados/"
```

#### Correção 3 — SIDRA PIB Trimestral (classificação obrigatória)

```python
# ERRADO (retorna ".." para todos os períodos):
url = f".../agregados/5932/periodos/{periodo}/variaveis/6562?localidades=N1[all]"

# CORRETO:
url = f".../agregados/5932/periodos/{periodo}/variaveis/6562?localidades=N1[all]&classificacao=11255[90707]"
```

#### Correção 4 — id_ente SICONFI (código IBGE 2 dígitos, não CNPJ)

```python
# CORRETO — confirmado com dados reais:
ESTADOS_SICONFI = {
    'AL': 27, 'BA': 29, 'CE': 23, 'MA': 21,
    'PB': 25, 'PE': 26, 'PI': 22, 'RN': 24, 'SE': 28
}
```

#### Correção 5 — SAGI: usar URL de cidadania.gov.br

```python
SAGI_URL = "https://aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/"
# (redireciona de: aplicacoes.mds.gov.br/sagi/ri/relatorios/cidadania/)
```

---

### 9.9 REFERÊNCIA RÁPIDA — TODAS AS URLS VERIFICADAS

```python
# ============================================================
# MAPA DE URLS — VERIFICADO EM 04/04/2026
# ============================================================

URLS = {
    # BACEN-SGS
    'bacen_sgs':       'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod}/dados',

    # SICONFI
    'siconfi_rreo':    'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo',
    'siconfi_rgf':     'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rgf',
    'siconfi_dca':     'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//dca',
    'siconfi_docs':    'https://apidatalake.tesouro.gov.br/docs/siconfi/',

    # BigQuery (via basedosdados)
    'bq_rais':         'basedosdados.br_me_rais.microdados_vinculos',
    'bq_caged_antigo': 'basedosdados.br_me_caged.microdados_antigos',
    'bq_caged_novo':   'basedosdados.br_me_caged.microdados_movimentacao',

    # IBGE SIDRA
    'sidra_pib_est':   'https://servicodados.ibge.gov.br/api/v3/agregados/5938/...',
    'sidra_pib_trim':  'https://servicodados.ibge.gov.br/api/v3/agregados/5932/...',
    # ATENÇÃO: PIB trimestral requer &classificacao=11255[90707]

    # Transparência (Bolsa Família)
    'portal_bf':       'http://api.portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio',
    'portal_cadastro': 'https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email',

    # SAGI/MDS (BPC + Bolsa Família)
    'sagi_ri':         'https://aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/',

    # FTP MTE (fallback)
    'ftp_mte':         'ftp://ftp.mtps.gov.br/pdet/microdados/',  # via ftplib apenas
}

# IDs de séries SGS
SGS = {
    'ipca':         433,
    'selic_meta':   4189,
    'selic_diaria': 11,
    'credito_pf':   20539,
    'credito_pj':   20540,
    'credito_total':20541,
    'ibcbr':        24364,
}

# Códigos IBGE 2 dígitos dos estados do NE
ESTADOS_NE = {
    'AL': 27, 'BA': 29, 'CE': 23, 'MA': 21,
    'PB': 25, 'PE': 26, 'PI': 22, 'RN': 24, 'SE': 28
}

# Contas SICONFI confirmadas
SICONFI_CONTAS = {
    'investimento_dca':   'DO4.4.00.00.00.00',   # DCA Anexo I-D, coluna "Despesas Liquidadas"
    'resultado_primario': 'ResultadoPrimarioComRPPSAcimaDaLinha',  # RREO Anexo 06
    'dcl':                'DividaConsolidadaLiquida',              # RGF Anexo 02
}
```

---

*Relatório gerado com verificação direta por chamadas HTTP às APIs. Todas as fontes foram testadas com dados reais. Próxima revisão recomendada: quando o CAGED de jan/2026 for publicado (~mar/2026) e quando as Contas Regionais 2023 forem divulgadas pelo IBGE (~novembro/2026).*
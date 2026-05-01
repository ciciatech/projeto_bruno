# Tese DESP/UFC — Impactos do Credito e do Emprego no Nordeste

> Documento consolidado do projeto de doutorado de Bruno Cardoso Costa.
> Gerado em: 15/04/2026 | Ultima atualizacao dos dados: 06/03/2026

---

## Ficha do Projeto

| Campo | Valor |
|-------|-------|
| **Autor** | Bruno Cardoso Costa |
| **Orientador** | Prof. Dr. Magno Prudencio de Almeida Filho |
| **Programa** | Doutorado Profissional em Economia do Setor Publico (DESP/UFC) |
| **Periodo de analise** | 2015-2025 |
| **Defesa prevista** | Marco/2028 |
| **Metodologia** | Wavelet (coerencia multipla e parcial) |
| **Pergunta central** | "Se dermos R$ 1 bilhao em credito, quantos mil empregos sao gerados?" |

---

## 1. Objetivo e Redirecionamento

### Objetivo original (Seminario de Tese)

Avaliar a influencia isolada do credito (PF e PJ) no crescimento economico (IBCR-NE) dos 9 estados do Nordeste, usando metodologia wavelet, com ~11 variaveis e dados bimestrais. Produto tecnologico: sistema inteligente de gestao com IA.

### Redirecionamento (fev/2025)

A variavel dependente migrou de IBCR-NE para **emprego** (formal + proxy informal). IBCR-NE passa a ser controle. Adicionadas transferencias federais (BPC, Bolsa Familia) e estaduais (SIOF-CE/PE/RN) como variaveis instrumentais.

### Estrutura revisada do modelo

| Papel | Variavel |
|-------|----------|
| **Dependente** | Volume de empregos (formais, com proxy para informais) |
| **Explicativa** | Credito PF (familias) — pelo lado da oferta |
| **Explicativa** | Credito PJ (empresas) — pelo lado da demanda |
| **Controle** | IBCR-NE, SELIC, IPCA, IBC-Br |
| **Controle** | Investimento publico, resultado primario, divida consolidada |
| **Controle** | Transferencias federais (BPC, Bolsa Familia) |
| **Controle** | Transferencias estaduais (SIOF) |

### Produto Tecnologico

| Dimensao | Proposito |
|----------|-----------|
| Tese academica | Artigo com aplicacao exemplificativa, comparacao BNB vs. BACEN |
| "Canhao" (produto real) | Ferramenta agnostica de gestao/predicao para BNB, secretarias, gestores |

---

## 2. Pipeline de Dados

### Arquitetura (4 camadas)

1. **extract** — coleta dados brutos de APIs, FTPs e portais publicos
2. **transform** — padroniza e organiza os dados em saidas por fonte, UF e tema (13 funcoes)
3. **preparacao_modelo** — deflacionamento e harmonizacao temporal (19 funcoes, 8 regras)
4. **quality** — auditoria automatizada de 19 datasets

### Extratores via CLI (`--modulos`)

| Modulo | Fonte | Saida principal |
|--------|-------|-----------------|
| `bacen` | BACEN-SGS | credito, IBCR-NE, IBC-Br, SELIC, IPCA, inadimplencia (13 series) |
| `siconfi_rreo` | SICONFI/STN | receitas, despesas e resultado primario (bimestral) |
| `siconfi_rgf` | SICONFI/STN | divida e gestao fiscal (quadrimestral) |
| `siconfi_dca` | SICONFI/STN | balanco patrimonial e investimento publico (anual) |
| `transferencias` | SICONFI/RREO | transferencias por whitelist auditavel (36 contas) |
| `bolsa_familia` | SAGI/MDS + Portal da Transparencia | coleta mensal por UF; fallback por URLs |
| `caged_rais` | FTP MTE/PDET | CAGED antigo (2015-2019), Novo CAGED (2020+), RAIS (2015-2022) |
| `auditoria_qualidade` | Camada interna | auditoria automatizada de 19 datasets |

### Extratores autonomos (via ETL)

| Extrator | Fonte | Saida |
|----------|-------|-------|
| `SiofCE` | SEPLAG/CE (WebForms ASP.NET) | execucao orcamentaria CE + obras por regiao/secretaria |
| `TransparenciaAL` | Portal Transparencia AL | execucao orcamentaria AL (2015-2025) |
| `TransparenciaPI` | Portal Transparencia PI | execucao orcamentaria PI (2015-2025) |

### Comandos de execucao

```bash
python3 -m pipeline.run --apenas-bacen          # coleta rapida
python3 -m pipeline.run                          # coleta principal
python3 -m pipeline.run --modulos bacen caged_rais  # modulos especificos
python3 -m pipeline.run --etl                    # apenas ETL
python3 -m pipeline.run --preparar-modelo        # apenas preparacao
python3 -m pipeline.run --auditar                # apenas auditoria
python3 -m pipeline.run --full                   # fluxo completo
python3 -m pipeline.run --modulos bolsa_familia --api-key CHAVE  # com API key
```

### Estrutura de saida

```
dados_nordeste/
+-- raw/                          # ~101 CSVs brutos por fonte
|   +-- bacen/nacional/           # Series BACEN-SGS
|   +-- caged/nordeste/           # CAGED antigo + novo + manifestos
|   +-- rais/nordeste/            # RAIS vinculos e setorial
|   +-- siconfi/nordeste/         # RREO, RGF, DCA
|   +-- transferencias/nordeste/  # Transferencias constitucionais
|   +-- bolsa_familia/nordeste/   # Bolsa Familia (quando ha API key)
|   +-- execucao_orcamentaria/    # CE (SIOF + obras), AL, PI
+-- processed/                    # ~134 CSVs tratados
|   +-- model_ready/              # Saidas finais para modelagem (9 arquivos)
+-- quality/                      # Relatorios de qualidade (JSON, MD, CSV)
+-- logs/ + metadata_coleta.json
```

---

## 3. Dados Brutos (raw)

### 3.1 BACEN — 13 Series Temporais SGS

| Codigo SGS | Nome | Unidade |
|-----------|------|---------|
| 25389 | IBCR_NE_ajuste_sazonal | Indice (2002=100) |
| 14084 | credito_PF_nordeste | R$ milhoes |
| 14089 | credito_PJ_nordeste | R$ milhoes |
| 14079 | credito_total_nordeste | R$ milhoes |
| 4189 | selic_mensal | % a.m. |
| 433 | ipca_mensal | % |
| 22109 | pib_trimestral_indice | Indice |
| 21084 | inadimplencia_PF | % |
| 21085 | inadimplencia_PJ | % |
| 20539 | credito_PF_brasil | R$ milhoes |
| 20541 | credito_PJ_brasil | R$ milhoes |
| 24364 | ibc_br | Indice (2002=100) |

Arquivos: `bacen_sgs_series.csv` (1.496 reg, long) e `bacen_sgs_wide.csv` (132 reg, wide). Periodo: jan/2015 a dez/2025, continuo.

### 3.2 CAGED Antigo (2015-2019)

Fonte: FTP MTE. Layout pre-eSocial. 423 registros (esperados 540 — 13 meses corrompidos no FTP).
Colunas: `ano`, `mes`, `sigla_uf`, `admissoes`, `desligamentos`, `saldo`, `salario_medio`, `total_movimentacoes`.

### 3.3 CAGED Novo (2020-2025)

Fonte: FTP MTE. Layout eSocial. 648 registros (completo). Mesma estrutura do antigo (harmonizado na coleta).

### 3.4 RAIS (2015-2022)

47 registros. **`remuneracao_media` 100% nula.** Anos 2019/2020 corrompidos. 2023 em formato `.COMT` (nao CSV).
Colunas: `ano`, `sigla_uf`, `vinculos_ativos`, `remuneracao_media`.

### 3.5 SICONFI — Dados Fiscais Estaduais

| Relatorio | Registros | Periodo | Frequencia |
|-----------|-----------|---------|------------|
| RREO | 1.987.525 | 2015-2025 | Bimestral x UF |
| RGF | 95.759 | 2015-2025 | Quadrimestral x UF |
| DCA | 232.905 | 2015-2024 | Anual x UF |

### 3.6 Transferencias Constitucionais

42.205 registros, 2015-2025, 9 UFs. Filtro por 36 contas: FPE, FPM, FUNDEB, CIDE, royalties.

### 3.7 Bolsa Familia

Depende de API key. Sem chave, gera 132 URLs de download como fallback. BPC ausente.

### 3.8 Execucao Orcamentaria Estadual

| Estado | Fonte | Registros | Periodo |
|--------|-------|-----------|---------|
| Ceara | SIOF (SEPLAG) | 480 | 2015-2026 |
| Ceara (obras) | SIOF rel. 110 | variavel | 2015-2026 |
| Alagoas | Portal Transparencia | 2.065 | 2015-2025 |
| Piaui | Portal Transparencia | 250 | 2015-2025 |

SIOF-CE tem 5 tipos de relatorio, 14 regioes, filtros por elemento de despesa (51-Obras, 52-Equipamentos).

---

## 4. Dados Processados (processed)

### 4.1 BACEN Tratado

- `bacen.csv` (132 reg) — series mensais originais
- `bacen_deflacionado.csv` (132 reg) — deflacionado IPCA base dez/2025=100
- `bacen_bimestral.csv` (66 reg) — harmonizado bimestral

### 4.2 CAGED Processado

- `caged_bimestral.csv` — **594 registros** (serie unificada 2015-2025, 9 UFs x 66 bimestres)
- `caged_por_setor.csv` — 4.263 reg (saldo por CNAE, 2020-2025)
- `caged_por_perfil.csv` — 1.317 reg (sexo x escolaridade, 2020-2025)
- Arquivos por UF individual

### 4.3 SICONFI Processado

- `rreo_resultado_primario.csv` — 36 reg por UF, bimestral
- `rgf_divida.csv` — 297 reg por UF, quadrimestral
- `dca_investimento.csv` — 20 reg por UF, anual

Contas-chave: `ResultadoPrimarioComRPPSAcimaDaLinha`, `DividaConsolidadaLiquida`, `DO4.4.00.00.00.00` (investimentos).

### 4.4 Execucao Orcamentaria Processada

- CE: `siof_ce.csv` + `siof_obras_secretaria.csv` + `siof_obras_regiao.csv`
- AL: `transparencia_al.csv`
- PI: `transparencia_pi.csv`

### 4.5 Painel Final (model_ready) — 9 arquivos

| Arquivo | Descricao |
|---------|-----------|
| **`painel_tese_bimestral.csv`** | **Base principal: 594 linhas, 37 colunas, 9.69% missing** |
| `bacen_bimestral.csv` | Series deflacionadas de credito |
| `caged_bimestral.csv` | Emprego por UF/bimestre |
| `resultado_primario_bimestral.csv` | Resultado primario por UF |
| `dcl_bimestral.csv` | Divida consolidada por UF |
| `investimento_publico_bimestral.csv` | Investimento publico por UF |
| `transferencias_bimestrais.csv` | Transferencias federais por UF |
| `rais_bimestral.csv` | Estoque de emprego formal |
| `matriz_regras_modelo.csv` | Documentacao das 8 regras de agregacao |

---

## 5. Regras do Modelo (MODEL_RULES)

| Variavel | Fonte | Agregacao bimestral | Deflacao | Unidade |
|----------|-------|---------------------|----------|---------|
| `saldo` | CAGED | soma mensal (fluxo) | Nao | postos de trabalho |
| `credito_PF_nordeste_real` | BACEN | ultimo valor (estoque) | IPCA | R$ dez/2025 milhoes |
| `credito_PJ_nordeste_real` | BACEN | ultimo valor (estoque) | IPCA | R$ dez/2025 milhoes |
| `resultado_primario_real` | RREO | acumulado no periodo | IPCA | R$ dez/2025 |
| `dcl_real` | RGF | repeticao quadrimestral | IPCA | R$ dez/2025 |
| `investimento_publico_real` | DCA | repeticao anual | IPCA | R$ dez/2025 |
| `transferencias_federais_real` | RREO | diferenca acumulada anual | IPCA | R$ dez/2025 |
| `rais_vinculos_ativos` | RAIS | repeticao anual (estoque) | Nao | vinculos formais |

### Mapeamento completo para o modelo wavelet

| Papel | Variavel | Arquivo | Status |
|-------|----------|---------|--------|
| Dependente | Emprego formal (fluxo) | `model_ready/caged_bimestral.csv` | **Completo 2015-2025** |
| Dependente | Emprego formal (estoque) | `model_ready/rais_bimestral.csv` | Parcial (6 de 8 anos) |
| Explicativa | Credito PF NE (real) | `model_ready/bacen_bimestral.csv` | Completo |
| Explicativa | Credito PJ NE (real) | `model_ready/bacen_bimestral.csv` | Completo |
| Controle | IBCR-NE, IBC-Br, SELIC, IPCA | `model_ready/bacen_bimestral.csv` | Completo |
| Controle | Resultado Primario | `model_ready/resultado_primario_bimestral.csv` | Completo (9 UFs) |
| Controle | DCL | `model_ready/dcl_bimestral.csv` | Completo |
| Controle | Investimento publico | `model_ready/investimento_publico_bimestral.csv` | Completo |
| Controle | Transferencias federais | `model_ready/transferencias_bimestrais.csv` | Completo |
| Controle | Bolsa Familia | `bolsa_familia_uf_mensal.csv` | Condicional (API key) |
| **Painel** | **Base model-ready** | **`painel_tese_bimestral.csv`** | **594 linhas, 37 colunas** |

---

## 6. Qualidade dos Dados

### Auditoria automatizada (19 datasets)

| Dataset | Camada | Status | Registros | Missing% | Alertas |
|---------|--------|--------|-----------|----------|---------|
| bacen_raw_wide | raw | ok | 132 | 5.13% | - |
| caged_antigo_raw | raw | alerta | 423 | 0% | 117 lacunas periodo-UF |
| caged_novo_raw | raw | ok | 648 | 0% | - |
| rais_vinculos_raw | raw | alerta | 47 | 25% | 2019/2020 ausentes |
| siconfi_rreo_raw | raw | ok | 1.987.525 | 1.57% | - |
| siconfi_rgf_raw | raw | ok | 95.253 | 0.92% | - |
| siconfi_dca_raw | raw | alerta | 232.661 | 1.33% | 2025 ausente |
| transferencias_raw | raw | ok | 42.205 | 1.7% | - |
| bolsa_familia_* | raw | alerta | 0 | - | Opcional, sem API key |
| bacen_bimestral | processed | ok | 66 | 1.75% | - |
| caged_bimestral | processed | ok | 594 | 0% | - |
| painel_tese_bimestral | processed | ok | 594 | 9.69% | - |

### Bug critico corrigido: `salario_medio`

O campo continha `total_movimentacoes` em 100% dos registros por bug de mojibake no encoding. Corrigido com `_fix_mojibake()`, mapeamento por substring e pre-deteccao de colunas prioritarias.

| Metrica | Antes | Depois |
|---------|-------|--------|
| Registros com salario (Antigo) | 0/423 | 423/423 |
| Registros com salario (Novo) | 18/648 | 648/648 |
| Mediana salario Antigo | N/A | R$ 1.207 |
| Mediana salario Novo | N/A | R$ 1.501 |

### CAGED Antigo — 13 meses ausentes (FTP corrompido)

| Ano | Meses faltantes |
|-----|-----------------|
| 2015 | jan, mar, jul, nov |
| 2016 | mar, mai |
| 2017 | abr, jun, dez |
| 2019 | jan, mar, ago, set |

Impacto: 20% dos bimestres 2015-2019 contem dados de apenas 1 mes (subestimacao ~60%).

### RAIS — Gaps severos

| Ano | Status |
|-----|--------|
| 2015 | Parcial (5 UFs corrompidas) |
| 2016-2017 | Parcial (1 UF corrompida cada) |
| 2018 | Completo (9/9) |
| 2019-2020 | **Falhou** (download corrompido) |
| 2021-2022 | Completo (9/9) |
| 2023 | Falhou (formato `.COMT`) |

`remuneracao_media` 100% nula em todos os 47 registros.

### Anomalias documentadas (nao sao erros)

- **Alagoas:** sazonalidade extrema em setembro (ciclo sucroalcooleiro, razao adm/desl > 2x)
- **COVID abr/2020:** queda abrupta em todos os estados (BA -35.098, CE -31.085, PE -26.212)
- **Sazonalidade NE:** pico em set (+7.104), vale em dez (-4.752)

### Harmonizacao de codificacao

- `sexo`: eSocial usa 3=feminino (antigo usa 2). Mapeamento `{3: 2}` aplicado.
- `grau_instrucao`: zero-padding variavel (`"01"` vs `"1"`). Normalizado para inteiro.

---

## 7. Participacao Regional do Emprego

| UF | Saldo acumulado 2015-2025 | Participacao |
|----|---------------------------|-------------|
| BA | +407.516 | 25.6% |
| CE | +336.092 | 21.1% |
| PE | +219.739 | 13.8% |
| MA | +151.378 | 9.5% |
| PB | +134.553 | 8.4% |
| RN | +117.746 | 7.4% |
| AL | +89.552 | 5.6% |
| PI | +86.466 | 5.4% |
| SE | +49.583 | 3.1% |
| **NE** | **+1.592.625** | **100%** |

---

## 8. Diagnostico do Projeto

### Avaliacao geral

| Eixo | Avaliacao |
|------|-----------|
| Proposito da tese | **Forte** — bem documentado e claro |
| Extracao das fontes centrais | **Forte** — BACEN, SICONFI, CAGED, SIOF funcionais |
| Variavel dependente | **Parcial** — CAGED forte, RAIS fragil |
| ETL | **Bom** — 13 funcoes de processamento |
| Harmonizacao para modelo | **Bom** — 8 regras no MODEL_RULES, painel gerado |
| Painel final | **Bom** — 594 linhas x 37 colunas, 9.69% missing |
| Execucao orcamentaria | **Parcial** — 3/9 estados (CE com obras, AL, PI) |
| Robustez para wavelet | **Parcial** — painel existe mas RAIS fragiliza estoque |

### Sintese

O projeto avancou da fase de "coleta modular" para "integracao analitica funcional". O painel bimestral final ja existe com 594 registros e 37 variaveis. O gap principal e RAIS (estoque de emprego) e transferencias assistenciais (BPC, Bolsa Familia).

---

## 9. Prioridades de Acao

| # | Acao | Status | Impacto |
|---|------|--------|---------|
| - | ~~Construir painel bimestral~~ | **FEITO** | `painel_tese_bimestral.csv` (594x37) |
| - | ~~Harmonizacao temporal completa~~ | **FEITO** | 8 regras no MODEL_RULES |
| 1 | Recolher RAIS via BigQuery (corrigir gaps 2019/2020) | Pendente | Estoque de emprego completo |
| 2 | Corrigir 13 meses CAGED Antigo (FTP corrompido) | Pendente | 20% dos bimestres parciais |
| 3 | Completar Bolsa Familia (SAGI + API key) e BPC | Pendente | Controles de transferencias |
| 4 | Solicitar SCR/BACEN via Prof. Magno | Pendente | Credito por CNAE/porte |
| 5 | Expandir exec. orcamentaria (BA, PE, PB, RN, SE, MA) | Pendente | 6 estados faltam |
| 6 | Coletar exportacao/importacao NE (MDIC/Comex Stat) | Pendente | Controle previsto na tese |
| 7 | Painel setorial (UF x setor x bimestre) | Pendente | Cruzamento CAGED x CNAE |

---

## 10. Oportunidades

1. **Cruzamento credito x emprego por setor** — CAGED traz saldo por CNAE; SCR viabilizaria painel setorial inedito
2. **Perfil do emprego** — sexo, escolaridade e faixa salarial ja disponiveis para analise distribucional
3. **Heterogeneidade estadual** — 9 estados + SICONFI permite investigar por que credito gera mais empregos em alguns estados
4. **4 regimes temporais** — recessao (2015-16), recuperacao (2017-19), pandemia (2020-21), retomada (2022-25)
5. **Execucao orcamentaria** — SIOF-CE com obras por regiao e Transparencia-AL sao raros na literatura

---

## 11. Referencia Tecnica

### URLs verificadas (04/2026)

```python
URLS = {
    'bacen_sgs':       'https://api.bcb.gov.br/dados/serie/bcdata.sgs.{cod}/dados',
    'siconfi_rreo':    'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rreo',
    'siconfi_rgf':     'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//rgf',
    'siconfi_dca':     'https://apidatalake.tesouro.gov.br/ords/siconfi/tt//dca',
    'bq_rais':         'basedosdados.br_me_rais.microdados_vinculos',
    'bq_caged_antigo': 'basedosdados.br_me_caged.microdados_antigos',
    'bq_caged_novo':   'basedosdados.br_me_caged.microdados_movimentacao',
    'sidra_pib_est':   'https://servicodados.ibge.gov.br/api/v3/agregados/5938/...',
    'sidra_pib_trim':  'https://servicodados.ibge.gov.br/api/v3/agregados/5932/...',
    'portal_bf':       'http://api.portaldatransparencia.gov.br/api-de-dados/...',
    'sagi_ri':         'https://aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/',
    'ftp_mte':         'ftp://ftp.mtps.gov.br/pdet/microdados/',
}
```

### Codigos de referencia

```python
SGS = {'ipca': 433, 'selic_meta': 4189, 'credito_pf': 20539,
       'credito_pj': 20540, 'credito_total': 20541, 'ibcbr': 24364}

ESTADOS_NE = {'AL': 27, 'BA': 29, 'CE': 23, 'MA': 21,
              'PB': 25, 'PE': 26, 'PI': 22, 'RN': 24, 'SE': 28}

SICONFI_CONTAS = {'investimento_dca': 'DO4.4.00.00.00.00',
                  'resultado_primario': 'ResultadoPrimarioComRPPSAcimaDaLinha',
                  'dcl': 'DividaConsolidadaLiquida'}
```

### CAGED/RAIS — BigQuery vs FTP

| Criterio | FTP direto | BigQuery (Base dos Dados) |
|----------|-----------|--------------------------|
| Custo | Gratis | Gratis (ate 1TB/mes) |
| Facilidade | Baixa | Alta (SQL direto) |
| Dados recentes | Sim (mensal) | Lag de semanas |
| Requer Google | Nao | Sim |

Recomendacao: BigQuery principal, FTP contingencia.

### Correcoes aplicadas/necessarias no pipeline

| Correcao | Detalhe |
|----------|---------|
| Nome tabela CAGED Novo | `microdados_movimentacao` (nao `microdados_novo_caged`) |
| URL Portal Transparencia | `http://` (nao `https://`) |
| SIDRA PIB Trimestral | Requer `&classificacao=11255[90707]` |
| id_ente SICONFI | Codigo IBGE 2 digitos (nao CNPJ) |
| SAGI URL | `aplicacoes.cidadania.gov.br` (redireciona de `mds.gov.br`) |

---

## 12. Changelog

### [2026-03-06] Qualidade, Model-Ready e SIOF Obras

- `pipeline/quality.py` — auditoria automatizada de 19 datasets
- Camada `model_ready` com painel bimestral final (594x37)
- SIOF-CE expandido: obras por regiao (14 regioes) e secretaria
- Transparencia PI adicionada como extrator
- RAIS corrigida para usar `vinculo_ativo`
- Transferencias com whitelist de 36 `cod_conta`
- Bolsa Familia com fallback SAGI/MDS
- `pipeline/run.py` com modos `--etl`, `--preparar-modelo`, `--auditar`, `--full`

### [2025-02-24] Redirecionamento da Tese

- Variavel dependente: IBCR-NE → emprego
- Pergunta: "Se dermos R$ 1 bi em credito, quantos mil empregos sao gerados?"
- Produto Tecnologico separado da tese (Prof. Paulo Matos)

---

## 13. Pontos de Atencao Metodologica

- **Variavel nao observavel:** informalidade como proxy requer justificativa metodologica
- **Dado subjetivo:** empregos em projetos especificos tem vies de mensuracao
- **Vies de ineditismo:** explorar o que ainda nao foi feito na literatura regional
- **Camada agnostica:** ferramenta deve funcionar independente da fonte de dados

---

## 14. Pendencias

| Item | Impacto |
|------|---------|
| RAIS 2019, 2020 corrompidos | Sem estoque de emprego para 2 anos |
| RAIS 2015 parcial (5 UFs) | Estoque parcial |
| RAIS 2023 formato `.COMT` | Sem estoque para 2023 |
| RAIS `remuneracao_media` 100% nula | Sem salario medio via RAIS |
| CAGED Antigo 13 meses corrompidos | 20% bimestres parciais |
| Bolsa Familia depende de API key | Controle condicional |
| BPC nao coletado | Controle ausente |
| Exportacao/Importacao NE ausente | Controle previsto ausente |
| Execucao orcamentaria 6/9 estados faltam | Cobertura parcial |

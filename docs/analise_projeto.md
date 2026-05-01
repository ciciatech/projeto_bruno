# Analise do Projeto — Tese DESP/UFC

**Tese:** Impactos do Credito no Crescimento Economico do Nordeste
**Autor:** Bruno Cardoso Costa
**Orientador:** Prof. Dr. Magno Prudencio de Almeida Filho
**Programa:** Doutorado Profissional em Economia do Setor Publico (DESP/UFC)
**Periodo de analise:** 2015-2025 | **Defesa prevista:** marco/2028

---

## 1. Objetivo e Redirecionamento

### Objetivo original (Seminario de Tese)

Avaliar a influencia isolada do credito (PF e PJ) no crescimento economico (IBCR-NE) dos 9 estados do Nordeste, usando metodologia wavelet (coerencia multipla e parcial), com ~11 variaveis e dados bimestrais. Produto tecnologico: sistema inteligente de gestao com IA.

### Redirecionamento (fev/2025)

A variavel dependente migrou de IBCR-NE para **emprego** (formal + proxy informal). Pergunta central:

> "Se dermos R$ 1 bilhao em credito, quantos mil empregos sao gerados?"

IBCR-NE passa a ser controle. Adicionadas transferencias federais (BPC, Bolsa Familia) e estaduais (SIOF-CE/PE/RN) como variaveis instrumentais.

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

O Prof. Paulo Matos estabeleceu separacao estrategica:

| Dimensao | Proposito |
|----------|-----------|
| Tese academica | Artigo com aplicacao exemplificativa, comparacao BNB vs. BACEN |
| "Canhao" (produto real) | Ferramenta agnostica de gestao/predicao para BNB, secretarias, gestores |

---

## 2. Inventario dos Dados Coletados

| Fonte | Arquivo Raw | Linhas | Periodo | 9 UFs |
|---|---|---|---|---|
| BACEN/SGS | `bacen_sgs_wide.csv` | 132 | jan/2015-dez/2025 | Nacional |
| CAGED Novo | `caged_saldo_mensal.csv` | 648 | jan/2020-dez/2025 | 9/9 |
| CAGED Antigo | `caged_antigo_saldo_mensal.csv` | 423 | fev/2015-dez/2019 | 9/9 |
| RAIS | `rais_vinculos.csv` | 47 | 2015-2022 (gaps) | 9/9 (parcial) |
| RAIS por Setor | `rais_por_setor.csv` | 3.733 | idem | idem |
| SICONFI DCA | `siconfi_dca_nordeste.csv` | 232.661 | 2015-2024 | 9/9 |
| SICONFI RREO | `siconfi_rreo_nordeste.csv` | 1.987.525 | 2015-2025 | 9/9 |
| SICONFI RGF | `siconfi_rgf_nordeste.csv` | 95.760 | 2015-2025 | 9/9 |
| Transferencias | `transferencias_constitucionais_nordeste.csv` | 42.205 | 2015-2025 | 9/9 |

Dados processados: 80+ arquivos por UF + 1 arquivo bimestral harmonizado (`caged_bimestral.csv`, 594 linhas, zero nulos).

---

## 3. Confronto: Requisitos da Tese vs. Dados Disponiveis

| Variavel prevista | Status | Gap |
|---|---|---|
| IBCR-NE (controle) | OK | Serie completa 2015-2025 |
| Credito PF Nordeste | OK | Serie mensal completa |
| Credito PJ Nordeste | OK | Serie mensal completa |
| Selic, IPCA, IBC-Br | OK | Series completas |
| Inadimplencia PF/PJ | OK | Series completas |
| Investimento publico (SICONFI) | OK | DCA 2015-2024 |
| Resultado primario (SICONFI) | OK | RREO 2015-2025 |
| Divida consolidada (SICONFI) | OK | RGF 2015-2025 |
| Transferencias federais | PARCIAL | Somente constitucionais via RREO. BPC/Bolsa Familia ainda nao coletados |
| Emprego formal (CAGED) | OK | 2015-2025 (saldo mensal) |
| Emprego formal (RAIS/estoque) | FRACO | Gaps extensos, sem remuneracao |
| SCR/BACEN (credito por CNAE/porte) | AUSENTE | Depende de acesso formal (Prof. Magno) |
| SIOF estaduais (CE/PE/RN) | PARCIAL | CE (SIOF + obras), AL e PI coletados; 6 estados faltam |
| Proxy informalidade | AUSENTE | Previsto como 3o momento |
| Exportacao/importacao NE | AUSENTE | Previsto na tese original |

---

## 4. Diagnostico Executivo

### Avaliacao geral

| Eixo | Situacao | Avaliacao |
|------|----------|-----------|
| Proposito da tese | Bem documentado e claro | Forte |
| Extracao das fontes centrais | BACEN, SICONFI, CAGED e SIOF funcionais | Forte |
| Variavel dependente principal | CAGED forte; RAIS fragil | Parcial |
| ETL por fonte | 13 funcoes de processamento, organiza bem | Bom |
| Harmonizacao para modelo | 8 regras definidas no MODEL_RULES, painel bimestral gerado | Bom |
| Painel final (model_ready) | `painel_tese_bimestral.csv`: 594 linhas, 37 colunas, 9.69% missing | Bom |
| Execucao orcamentaria | CE expandido com obras/regioes; AL e PI funcionais | Parcial |
| Robustez para wavelet | Painel existe mas RAIS fragiliza cobertura de estoque | Parcial |

### Sintese

O projeto avancou significativamente na camada de integracao analitica. O painel bimestral final ja existe com 594 registros e 37 variaveis. O gap principal permanece na RAIS (estoque de emprego) e na cobertura de transferencias assistenciais (BPC, Bolsa Familia).

---

## 5. Avaliacao por Eixo

### 5.1 BACEN — Forte

Parte mais madura. Series centrais completas 2015-2025. Expansao assimetrica ja visivel: credito PF cresceu ~240% (R$283bi -> R$678bi) enquanto PJ cresceu menos. O problema aqui nao e coleta, e sim integracao com as demais fontes.

### 5.2 CAGED e RAIS — Coracao do projeto

**Pontos fortes:** pipeline ja incorporou mudanca de foco para emprego; continuidade 2015-2025 do fluxo conceitualmente bem desenhada; agregacoes por UF, setor e perfil disponiveis.

**Fragilidades:** estrategia principal depende do FTP do MTE (menos robusto que BigQuery/Base dos Dados); RAIS incompleta e com baixa confiabilidade; 13 meses corrompidos no CAGED Antigo.

**Conclusao:** Alinhamento conceitual forte, robustez operacional parcial. A variavel dependente precisa ser a parte mais confiavel, e hoje ainda nao e.

### 5.3 SICONFI — Muito bom

Cobertura ampla por UF e ano. Boa aderencia aos controles fiscais. ETL extrai resultado primario (RREO Anexo 06), divida (RGF Anexo 02) e investimento (DCA Anexo I-D). Harmonizacao bimestral ja implementada no `preparacao_modelo.py` para as 3 variaveis.

### 5.4 Transferencias — Parcial

Transferencias constitucionais via RREO coletadas com whitelist auditavel de 36 contas. Bolsa Familia tem integracao com SAGI/MDS + fallback para Portal de Dados Abertos, mas coleta efetiva depende de API key. BPC ausente.

### 5.5 Execucao orcamentaria estadual — Diferencial em evolucao

Cobertura expandida para 3 estados: CE (SIOF com 5 tipos de relatorio + coleta especifica de obras por regiao e secretaria), AL (Portal Transparencia) e PI (Portal Transparencia). Schemas diferentes entre fontes, com funcao de alias no ETL para facilitar comparacao. 6 estados ainda sem dados (BA, PE, PB, RN, SE, MA).

---

## 6. Avaliacao do ETL e Preparacao para Modelo

### ETL (`pipeline/transform/etl.py` — 13 funcoes)

| Funcao | Fonte | Saida |
|--------|-------|-------|
| `processar_bacen` | BACEN SGS | Series harmonizadas |
| `processar_bolsa_familia` | Bolsa Familia | Split por estado |
| `processar_rreo` | SICONFI RREO | Resultado primario (13 contas-chave) |
| `processar_rgf` | SICONFI RGF | Divida consolidada |
| `processar_dca` | SICONFI DCA | Investimento publico |
| `processar_transferencias` | RREO Anexos 01/06 | Transferencias constitucionais |
| `processar_siof` | SIOF-CE | Execucao orcamentaria CE |
| `processar_siof_obras` | SIOF-CE (rel. 110) | Obras por secretaria e regiao |
| `processar_transparencia_al` | Portal AL | Execucao orcamentaria AL |
| `processar_transparencia_pi` | Portal PI | Execucao orcamentaria PI |
| `processar_caged_antigo` | FTP MTE | Saldo mensal por UF 2015-2019 |
| `processar_caged` | FTP MTE | Saldo mensal por UF 2020+ |
| `processar_rais` | FTP MTE | Vinculos ativos por UF/ano |

### Preparacao para modelo (`preparacao_modelo.py` — 19 funcoes)

**Funcionalidades implementadas:**
- Deflacionamento pelo IPCA (base dez/2025=100) — funcoes generica, bimestral e anual
- Harmonizacao mensal→bimestral (BACEN, CAGED)
- Harmonizacao quadrimestral→bimestral (RGF/DCL — repeticao)
- Harmonizacao anual→bimestral (DCA/investimento, RAIS — repeticao)
- Transferencias: diferenca acumulada anual para bimestral
- Construcao do `painel_tese_bimestral.csv` via join de todas as fontes
- `matriz_regras_modelo.csv` documentando as 8 regras de agregacao

### O que ainda pode melhorar

- Validacao automatica de coerencia entre fontes no painel final
- Selecao definitiva de variaveis com dicionario fixo exportavel
- Painel setorial `UF x setor x bimestre` (cruzamento CAGED x CNAE)

---

## 7. Oportunidades Identificadas

1. **Cruzamento credito x emprego por setor:** CAGED traz saldo por CNAE. Se SCR for viabilizado, painel setorial inedito.

2. **Dimensao perfil do emprego:** Sexo, escolaridade e faixa salarial ja disponiveis. Analise distribucional do efeito do credito.

3. **Heterogeneidade estadual:** 9 estados + SICONFI permite investigar por que credito gera mais empregos em alguns estados. Dialoga com literatura de convergencia regional.

4. **Janela temporal rica:** 4 regimes distintos (recessao 2015-16, recuperacao 2017-19, pandemia 2020-21, retomada 2022-25). Wavelet e adequado para capturar mudancas entre regimes.

5. **Execucao orcamentaria como ineditismo:** SIOF-CE e Transparencia-AL com granularidade de empenho/pagamento sao raros na literatura.

---

## 8. Prioridades de Acao

| Prioridade | Acao | Status | Impacto |
|------------|------|--------|---------|
| ~~3~~ | ~~Construir painel bimestral UF x tempo x variaveis~~ | **FEITO** | `painel_tese_bimestral.csv` (594x37) |
| ~~6~~ | ~~Harmonizacao temporal (DCL, investimento, RAIS para bimestral)~~ | **FEITO** | 8 regras no MODEL_RULES |
| **1 (urgente)** | Recolher RAIS via BigQuery/Base dos Dados (corrigir gaps 2019/2020) | Pendente | Estoque de emprego completo |
| **2 (urgente)** | Corrigir 13 meses do CAGED Antigo (FTP corrompido) | Pendente | 20% dos bimestres 2015-2019 parciais |
| **3 (alta)** | Completar Bolsa Familia (coletar via SAGI com API key) e BPC | Pendente | Controles de transferencias |
| **4 (alta)** | Solicitar SCR/BACEN via Prof. Magno | Pendente | Credito por CNAE/porte |
| **5 (media)** | Expandir execucao orcamentaria para BA, PE, PB, RN, SE, MA | Pendente | 6 estados faltam |
| **6 (media)** | Coletar exportacao/importacao NE (MDIC/Comex Stat) | Pendente | Controle previsto na tese |
| **7 (baixa)** | Painel setorial (UF x setor x bimestre) | Pendente | Cruzamento CAGED x CNAE |

---

## 9. Arquitetura Recomendada

### Camadas de dados

| Camada | Descricao |
|--------|-----------|
| **Raw** | Dados brutos imutaveis por fonte, periodo e versao |
| **Staging** | Dados padronizados com schema estavel, tipos corrigidos, chaves explicitas |
| **Analytical** | Paineis prontos para cruzamento (UF x bimestre, UF x setor x bimestre) |
| **Model-ready** | Base final com selecao definitiva das variaveis da tese |

### Chave analitica principal

- `uf` + `ano_bim` + `bimestre`
- Versao setorial: + `divisao_cnae`

### Contratos de dados

Cada variavel critica deve ter: definicao economica, fonte oficial, unidade de medida, frequencia original, regra de agregacao para bimestre, regra de deflacionamento, regra de imputacao/interpolacao, teste minimo de qualidade.

---

## 10. Referencia Tecnica de APIs e Fontes

### 10.1 URLs Verificadas (04/2026)

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
    'portal_bf':       'http://api.portaldatransparencia.gov.br/api-de-dados/bolsa-familia-por-municipio',
    'sagi_ri':         'https://aplicacoes.cidadania.gov.br/ri/ri/relatorios/cidadania/',
    'ftp_mte':         'ftp://ftp.mtps.gov.br/pdet/microdados/',
}
```

### 10.2 Codigos de referencia

```python
# Series SGS (BACEN)
SGS = {
    'ipca': 433, 'selic_meta': 4189, 'credito_pf': 20539,
    'credito_pj': 20540, 'credito_total': 20541, 'ibcbr': 24364,
}

# Codigos IBGE dos estados NE
ESTADOS_NE = {
    'AL': 27, 'BA': 29, 'CE': 23, 'MA': 21,
    'PB': 25, 'PE': 26, 'PI': 22, 'RN': 24, 'SE': 28
}

# Contas SICONFI confirmadas
SICONFI_CONTAS = {
    'investimento_dca':   'DO4.4.00.00.00.00',
    'resultado_primario': 'ResultadoPrimarioComRPPSAcimaDaLinha',
    'dcl':                'DividaConsolidadaLiquida',
}
```

### 10.3 Fontes alternativas para CAGED/RAIS

| Criterio | FTP direto | Base dos Dados (BigQuery) |
|----------|-----------|--------------------------|
| Custo | Gratis | Gratis (ate 1TB/mes) |
| Facilidade | Baixa (arquivo pesado) | Alta (SQL direto) |
| Dados mais recentes | Sim (mensal) | Pode ter lag de semanas |
| Requer conta Google | Nao | Sim |
| RAIS Nordeste | Arquivo dedicado pronto | Query SQL |

**Recomendacao:** BigQuery como rota principal; FTP como contingencia.

### 10.4 Correcoes necessarias no pipeline

| Correcao | Detalhe |
|----------|---------|
| Nome tabela CAGED Novo | `microdados_movimentacao` (nao `microdados_novo_caged`) |
| URL Portal Transparencia | `http://` (nao `https://`) |
| SIDRA PIB Trimestral | Requer `&classificacao=11255[90707]` |
| id_ente SICONFI | Codigo IBGE 2 digitos (nao CNPJ) |
| SAGI URL | `aplicacoes.cidadania.gov.br` (redireciona de `mds.gov.br`) |

---

## 11. Conclusao

O projeto avancou significativamente desde a fase de "coleta modular" para uma "integracao analitica funcional". O painel bimestral final ja existe (594 linhas, 37 colunas), com deflacionamento, harmonizacao temporal e 8 regras de agregacao formalizadas. A coleta de execucao orcamentaria expandiu para 3 estados, com o SIOF-CE agora incluindo dados de obras por regiao e secretaria.

Os gaps remanescentes sao concentrados: RAIS (estoque de emprego com lacunas em 2019/2020), CAGED Antigo (13 meses corrompidos no FTP), e transferencias assistenciais (BPC ausente, Bolsa Familia pendente de API key). Resolver esses pontos elevaria o projeto ao nivel de robustez necessario para a execucao final do modelo wavelet.

### Pontos de atencao metodologica

- **Variavel nao observavel:** informalidade como proxy requer justificativa metodologica
- **Dado subjetivo:** empregos em projetos especificos tem vies de mensuracao
- **Vies de ineditismo:** explorar o que ainda nao foi feito na literatura regional
- **Camada agnostica:** ferramenta deve funcionar independente da fonte de dados

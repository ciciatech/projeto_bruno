# FASE 6: MODELO DE DOMÍNIO — O que o sistema faz?

## Glossário de Entidades

### Entidades Principais

| Entidade | Sinônimos | Definição | Fonte | Periodicidade |
|----------|-----------|-----------|-------|---------------|
| **Estado (UF)** | Unidade Federativa | AL, BA, CE, MA, PB, PE, PI, RN, SE | Config | — |
| **Período** | Data, Mês, Bimestre | 2015-01-01 até 2025-12-31 | BACEN | Mensal (principal) |
| **Emprego Formal** | Vínculos ativos, CAGED | Número de pessoas com contrato de trabalho formal | CAGED + RAIS | Mensal (saldo) |
| **Crédito** | Saldo de crédito | Valor em R$ de operações de crédito vigentes | BACEN | Mensal |
| **Indicador Econômico** | Série temporal | IBCR-NE, SELIC, IPCA, IBC-Br, inadimplência | BACEN | Mensal |
| **Receita Orçamentária** | Ingressos, Arrecadação | Recursos que entram no governo (imposto, taxa, etc) | SICONFI | Bimestral |
| **Despesa Orçamentária** | Saída, Gasto | Recursos que saem do governo | SICONFI | Bimestral |
| **Resultado Primário** | Saldo, Necessidade de Financiamento | Receita - Despesa sem juros | SICONFI | Bimestral |
| **Dívida Consolidada** | Estoque de dívida | Saldo total de dívida do governo | SICONFI | Quadrimestral |
| **Investimento Público** | Capex, Formação de capital | Despesa com ativo fixo (obras, equipamentos) | SICONFI | Anual |
| **Transferência** | FPE, FUNDEB, ICM | Repasses constitucionais ou discricionários | SICONFI | Bimestral |
| **Execução Orçamentária** | Execução, Despesa liquidada | Efetivamente gasto (não apenas orçado) | Portal Transparência, SIOF | Anual |

---

### Medidas Derivadas

| Medida | Fórmula | Significado | Uso |
|--------|---------|-----------|-----|
| **Crédito per capita** | Crédito / População | Intensidade de crédito | Normalização |
| **Elasticidade crédito-emprego** | ΔEmprego / ΔCrédito | Quantos empregos por real de crédito | Modelo principal |
| **Resultado primário / PIB** | Resultado / PIB | Sustentabilidade fiscal | Controle |
| **Taxa de inadimplência** | Crédito inadimplente / Crédito total | Qualidade de crédito | Risco |
| **Valor médio de transferência** | Transferência total / População | Alcance de políticas sociais | Controle |

---

## Entidades Conceituais (Economia Pública)

```
┌──────────────────────────────────────────────────────┐
│           REGIÃO (Nordeste — 9 Estados)              │
├──────────────────────────────────────────────────────┤
│                                                       │
│  ┌─────────────────┐      ┌─────────────────┐       │
│  │  SETOR EXTERNO  │      │ SETOR PÚBLICO   │       │
│  │                 │      │                 │       │
│  │ · BACEN         │      │ · SICONFI       │       │
│  │   (Crédito)     │      │   (Fisco)       │       │
│  │ · Inadimpl.     │      │ · Investimento  │       │
│  │ · IBCR-NE       │      │ · Transferências│       │
│  │   (Atividade)   │      │                 │       │
│  └─────────────────┘      └─────────────────┘       │
│           │                        │                 │
│           └──────────────┬─────────┘                 │
│                          │                           │
│                   Variável Dependente                │
│                   ┌────────────────┐                 │
│                   │ EMPREGO FORMAL │                 │
│                   │ (CAGED + RAIS) │                 │
│                   └────────────────┘                 │
│                                                       │
└──────────────────────────────────────────────────────┘

Pergunta Central:
  "Se dermos R$ 1 bilhão em crédito ao Nordeste,
   quantos mil empregos formais serão criados,
   controlando por macroeconomia e finanças públicas?"
```

---

## Modelo Econométrico (Conceitual)

```
LOG(EmpregoPt,s) = β₀ + β₁·LOG(CréditoPFt,s) + β₂·LOG(CréditoPJt,s)
                   + γ₁·IBCRt + γ₂·SELICt + γ₃·IPCAt
                   + δ₁·ResultadoPrimáriot,s + δ₂·DívdaCont.t,s
                   + ξ₁·Transferênciast,s
                   + εt,s

Onde:
  t = tempo (bimestral, 2015-2025)
  s = estado (9 UFs)
  P = Pessoa Física (crédito ao consumidor)
  J = Pessoa Jurídica (crédito ao empresário)
```

**Variáveis:**
- Y (dependente): Volume de empregos (formais)
- X₁ (explicativa): Crédito PF (familias consomem, demanda de trabalho)
- X₂ (explicativa): Crédito PJ (empresas expandem, oferta de trabalho)
- Z (controles): IBCR (atividade real), SELIC (custo do crédito), IPCA (inflação)
- W (controles): Resultado fiscal, dívida, transferências (políticas públicas)

**Hipótese:** β₁ > 0, β₂ > 0 (crédito gera emprego)

---

## Fluxo de Regressão Econômica (Conceitual)

```
Step 1: Coleta
  ├─ Y: Emprego (CAGED/RAIS)
  ├─ X₁, X₂: Crédito PF/PJ (BACEN)
  ├─ Z, W: Controles (BACEN, SICONFI)
  └─ Output: painel_tese_bimestral.csv (594 × 37)

Step 2: Verificação (Fora do escopo código)
  ├─ Estacionariedade (ADF test)
  ├─ Cointegração (Johansen)
  ├─ Autocorrelação (Durbin-Watson)
  └─ Heterocedasticidade (White test)

Step 3: Especificação
  ├─ OLS vs. FE vs. RE (Hausman test)
  ├─ Lag structure (Akaike, Schwarz)
  ├─ Instrumentalização (se crédito endógeno)
  └─ Wavelet (metodologia semestral de Tese original)

Step 4: Resultados
  └─ β₁, β₂ significativos? Elasticidade crédito-emprego
```

---

## Estrutura de Dados do Painel

```
painel_tese_bimestral.csv

Índice (594 linhas):
  UF × Período = 9 UFs × 66 bimestres (2015-2025)

Colunas (37 + 2 ID):
  
  ID:
  ├─ data (YYYY-MM-DD, bimestral)
  └─ uf (AL, BA, CE, ...)
  
  Y (Variável Dependente):
  └─ emprego_formal_bimestral (volume estoque CAGED+RAIS)
  
  X (Explicativas):
  ├─ credito_pf_ne (crédito PF, bimestral média)
  └─ credito_pj_ne (crédito PJ, bimestral média)
  
  Z (Controles Macro):
  ├─ ibcr_ne (atividade produtiva)
  ├─ selic (taxa de juros)
  ├─ ipca (inflação)
  ├─ ibc_br (atividade Brasil)
  ├─ inadimpl_pf (risco PF)
  └─ inadimpl_pj (risco PJ)
  
  W (Controles Fiscal/Social):
  ├─ resultado_primario (receita - despesa)
  ├─ divida_consolidada (estoque)
  ├─ investimento_publico (capex)
  ├─ transferencias_fpe (FPE)
  ├─ transferencias_fundeb (FUNDEB)
  ├─ bolsa_familia (estimado, parcial)
  └─ bpc (ausente — seria BPC federal)
  
  [+ variáveis auxiliares de deflacionamento, gaps]
```

---

## Casos de Uso (Personas)

### Persona 1: Bruno Cardoso (Doutorando)

**Objetivo:** Escrever tese mostrando impacto de crédito em emprego

**Fluxo:**
```
1. Executa: python3 -m pipeline.run --full
2. Aguarda ~3h (coleta + ETL + quality)
3. Abre Jupyter: model_analysis.ipynb
4. Carrega: dados_nordeste/processed/model_ready/painel_tese_bimestral.csv
5. Roda regressão usando statsmodels/scikit-learn
6. Testa: elasticidade crédito-emprego significativa?
7. Escreve: capítulo de resultados
```

**Necessidades:**
- Dados limpos, sem gaps invisíveis (✓ quality report)
- Série contínua 2015-2025 (⚠️ RAIS/CAGED têm gaps)
- Documentação clara de deflacionamento (⚠️ Parcial em código)

### Persona 2: Prof. Magno (Orientador)

**Objetivo:** Validar metodologia, revisar resultados

**Fluxo:**
```
1. Recebe painel em Excel
2. Abre no Stata/R
3. Roda regressão própria
4. Compara com resultados Bruno
5. Avalia: estacionariedade, cointegração, heterocedasticidade?
```

**Necessidades:**
- Descrição clara de variáveis (✓ dicionario_dados.md)
- Documentação de transformações (⚠️ Parcial)
- Rastreabilidade de dados (⚠️ Sem versioning)

### Persona 3: Prof. Paulo Matos (Produto Tecnológico)

**Objetivo:** Usar pipeline para criar sistema de gestão para BNB/governo

**Fluxo:**
```
1. Fork do projeto_bruno
2. Adapta pipeline para dados BNB (via API SCR)
3. Cria dashboard customizado (secretarias, gestores)
4. Deploy em servidor BNB
```

**Necessidades:**
- Código modular e bem documentado (✓ Extract/Transform bem separados)
- Fácil adicionar novo coletor (✓ Padrão claro)
- Sem dependências hardcoded (⚠️ config.py é rígido)

---

## Regras de Negócio (Domain Rules)

### Regra 1: Cobertura Regional

```
Cobertura = {AL, BA, CE, MA, PB, PE, PI, RN, SE} (Nordeste completo)
Exceção: Execução Orçamentária apenas CE, AL, PI (6 UFs faltam)
```

### Regra 2: Período de Análise

```
Análise = 2015-2025 (11 anos)
Defesa = março/2028 (dados até 2027-12-31 esperado)
Gap: 2019-2020 em RAIS/CAGED — tratado com cautela
```

### Regra 3: Variável Dependente

```
Emprego_Formal = CAGED (saldo mensal 2015-2025) 
                 + RAIS (estoque anual 2015-2022)
Harmonização: Ambos para bimestral (agregação)
Proxy informalidade: AUSENTE (seria 3º momento de tese)
```

### Regra 4: Deflacionamento

```
Valores Reais = Valores Nominais / (IPCA_acumulado / 100)
Base = 2015-01-01
Aplicado em: Crédito (BACEN), Transferências, Investimento Público
Não aplicado em: Índices (IBCR), Taxas (SELIC), Quantidades (Emprego)
```

### Regra 5: Agregação Temporal

```
Mensal → Bimestral:
  · Stock vars (estoque): Usar último valor do período
  · Flow vars (fluxo): Usar soma do período
  
Exemplo:
  · Estoque emprego: médico (estoque)
  · Crédito: média (estoque)
  · Resultado primário: soma (fluxo)
```

---

## Constrains e Limitações

| Constraint | Descrição | Impacto |
|-----------|-----------|---------|
| **Sem BPC/Bolsa Familia 100%** | Apenas dados parciais via Portal Transparência | Variável de controle (transferência) incompleta |
| **RAIS sem remuneração** | Campo inteiro NULL | Proxy salarial inviável |
| **CAGED 13 gaps** | 13 de 60 meses (2015-2019) faltando | Interpolação necessária, viés possível |
| **Sem dados SCR** | Crédito não desagregado por CNAE/porte | Não pode validar "crédito agrícola gera emprego agrícola" |
| **Execução orch. 3/9** | Apenas CE, AL, PI cobertos | Proxy de investimento público incompleto |
| **Sem causalidade comprovada** | Correlação ≠ causação | Metodologia wavelet (coerência) tenta endereçar |

---

## Conclusão da Fase 6

**Domínio:** Bem definido (tese de economia pública é clara)  
**Modelo:** Claro (regressão logarítmica padrão)  
**Dados:** Cobertura boa (9/9 UFs), mas com gaps e limitações  
**Risco semântico:** Médio (agregação bimestral não documentada no código)

---

## Resumo: Mapa Mental da Solução

```
╔════════════════════════════════════════════════════════════════════════╗
║                     PROJETO BRUNO — TESE DESP/UFC                    ║
╠════════════════════════════════════════════════════════════════════════╣
║                                                                         ║
║  PERGUNTA: "R$ 1bi em crédito → quantos empregos no Nordeste?"       ║
║                                                                         ║
║  ┌─────────────────────────────────────────────────────────────────┐  ║
║  │ DADO                                                             │  ║
║  │ ├─ Y: Emprego (CAGED + RAIS, 2015-2025, 9 UFs, bimestral)     │  ║
║  │ ├─ X: Crédito (BACEN, 2015-2025, 9 UFs, bimestral)           │  ║
║  │ └─ Z,W: Controles (Macro + Fiscal, vários)                   │  ║
║  │    └─ Painel final: 594 linhas × 37 colunas                  │  ║
║  │                                                                │  ║
║  │ STATUS: 85% OK (RAIS gaps, CAGED gaps, SIOF 3/9)             │  ║
║  └─────────────────────────────────────────────────────────────────┘  ║
║                                                                         ║
║  ┌─────────────────────────────────────────────────────────────────┐  ║
║  │ ANÁLISE (Fora do código, statsmodels/Stata)                   │  ║
║  │ ├─ OLS: Elasticidade crédito-emprego (β coeff)               │  ║
║  │ ├─ Wavelet: Coerência parcial (tese original)                │  ║
║  │ └─ Robustness: IV, FE, RE, lag structure                     │  ║
║  └─────────────────────────────────────────────────────────────────┘  ║
║                                                                         ║
║  ┌─────────────────────────────────────────────────────────────────┐  ║
║  │ PRODUTO SECUNDÁRIO (Prof. Paulo Matos)                         │  ║
║  │ ├─ Ferramenta de gestão (BNB, secretarias)                    │  ║
║  │ ├─ Predição de emprego dado crédito                           │  ║
║  │ └─ Dashboard com dados em tempo real (outro escopo)           │  ║
║  └─────────────────────────────────────────────────────────────────┘  ║
║                                                                         ║
╚════════════════════════════════════════════════════════════════════════╝
```


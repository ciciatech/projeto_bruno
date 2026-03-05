# Relatório de Análise — Projeto de Doutorado Bruno Cardoso Costa

## 1. Objetivo do Projeto

**Tese:** "Impactos do Crédito no Crescimento Econômico do Nordeste"
**Programa:** Doutorado Profissional em Economia do Setor Público (DESP/UFC)
**Orientador:** Prof. Dr. Magno Prudêncio de Almeida Filho
**Período de análise:** 2015–2025 | **Defesa prevista:** março/2028

### Objetivo original (Seminário de Tese)
Avaliar a influência **isolada** do crédito (PF e PJ) no crescimento econômico (IBCR-NE) dos 9 estados do Nordeste, usando **metodologia wavelet** (coerência múltipla e parcial), com ~11 variáveis e dados bimestrais. Produto tecnológico: sistema inteligente de gestão com IA.

### Redirecionamento (changelog, fev/2025)
A variável dependente migrou de **IBCR-NE** para **emprego** (formal + proxy informal), respondendo: *"Se dermos R$ 1 bi em crédito, quantos mil empregos são gerados?"*. IBCR-NE passa a ser controle. Foram adicionadas transferências federais (BPC, Bolsa Família) e estaduais (SIOF-CE/PE/RN) como variáveis instrumentais.

---

## 2. Inventário dos Dados Coletados (`dados_nordeste/`)

| Fonte | Arquivo Raw | Linhas | Período | 9 UFs |
|---|---|---|---|---|
| **BACEN/SGS** | `bacen_sgs_wide.csv` | 132 | jan/2015–dez/2025 | Nacional (NE nas séries) |
| **CAGED Novo** | `caged_saldo_mensal.csv` | 648 | jan/2020–dez/2025 | 9/9 |
| **CAGED Antigo** | `caged_antigo_saldo_mensal.csv` | 423 | fev/2015–dez/2019 | 9/9 |
| **RAIS** | `rais_vinculos.csv` | 47 | 2015–2022 (gaps) | 9/9 (parcial) |
| **RAIS por Setor** | `rais_por_setor.csv` | 3.733 | idem | idem |
| **SICONFI DCA** | `siconfi_dca_nordeste.csv` | 232.661 | 2015–2024 | 9/9 |
| **SICONFI RREO** | `siconfi_rreo_nordeste.csv` | 1.987.525 | 2015–2025 | 9/9 |
| **SICONFI RGF** | `siconfi_rgf_nordeste.csv` | 95.760 | 2015–2025 | 9/9 |
| **Transferências** | `transferencias_constitucionais_nordeste.csv` | 42.205 | 2015–2025 | 9/9 |

**Dados processados:** 80+ arquivos por UF (CAGED/RAIS/SICONFI/Transferências) + 1 arquivo bimestral harmonizado (`caged_bimestral.csv`, 594 linhas, zero nulos).

---

## 3. Análise de Qualidade dos Dados

### 3.1 BACEN/SGS — Qualidade: BOA
- 132 meses (jan/2015–dez/2025), cobertura completa
- **Zero nulos** em: IBCR-NE, crédito PF/PJ (NE e BR), Selic, IPCA, IBC-Br, inadimplência
- **88 nulos no PIB trimestral** — esperado (dado é trimestral, publicado em 1 mês a cada 3)
- Todas as 12 séries previstas na tese estão presentes

### 3.2 CAGED (Novo + Antigo) — Qualidade: BOA (com ressalva)
- **Continuidade perfeita:** Antigo termina em dez/2019, Novo começa em jan/2020
- Todos os 9 estados cobertos, zero nulos
- Bimestralização completa (594 registros, 66 bimestres x 9 UFs)
- **Ressalva:** CAGED Antigo começa em fev/2015 (jan/2015 ausente — erro `Corrupt input data` no FTP, logs confirmam). Isso afeta o 1o bimestre de 2015, que contém apenas 1 mês para o CAGED Antigo

### 3.3 RAIS — Qualidade: FRACA (problema mais grave)
- **`remuneracao_media` 100% nula** em todos os registros (47/47) — campo completamente vazio
- **Gaps significativos por UF:**

| Gap | UFs afetadas |
|---|---|
| **2019 e 2020 ausentes** | Todas as 9 UFs |
| **2015 ausente** | BA, CE, PB, PE, PI |
| **2016 ausente** | PE |
| **2017 ausente** | BA |
| **2023–2025 ausentes** | Todas (RAIS só vai até 2022) |

- Cobertura varia de **4 anos** (BA, PE) a **6 anos** (AL, MA, RN, SE) — longe dos 8 esperados (2015–2022)
- Como a **variável dependente agora é emprego**, esta é a lacuna mais crítica do dataset

### 3.4 SICONFI (DCA + RREO + RGF) — Qualidade: MUITO BOA
- **DCA:** 2015–2024, 9 UFs completas (10 anos cada), zero nulos nos valores
- **RREO:** 2015–2025, 6 bimestres/ano, 9 UFs, ~2M linhas, zero nulos
- **RGF:** 2015–2025, quadrimestral, 9 UFs, zero nulos
- Variáveis fiscais (investimento, resultado primário, dívida) estão disponíveis conforme previsto na tese
- Processamento por UF correto e consistente (DCA Invest: 20 linhas/UF, DCA Resumo: 70/UF)

### 3.5 Transferências Constitucionais — Qualidade: BOA
- 2015–2025, 9 UFs, zero nulos
- Extraídas do RREO (transferências correntes e intergovernamentais)
- Cobertura varia de 572 (SE) a 821 (PE) registros processados por UF — variação decorre de diferenças na abertura contábil

---

## 4. Confronto: Requisitos da Tese vs. Dados Disponíveis

| Variável prevista | Status | Observação |
|---|---|---|
| IBCR-NE (controle) | OK | Série completa 2015–2025 |
| Crédito PF Nordeste | OK | Série mensal completa |
| Crédito PJ Nordeste | OK | Série mensal completa |
| Selic, IPCA, IBC-Br | OK | Séries completas |
| Inadimplência PF/PJ | OK | Séries completas |
| Investimento público (SICONFI) | OK | DCA 2015–2024 |
| Resultado primário (SICONFI) | OK | RREO 2015–2025 |
| Dívida consolidada (SICONFI) | OK | RGF 2015–2025 |
| Transferências federais | PARCIAL | Somente constitucionais via RREO. BPC/Bolsa Família (changelog) **ainda não coletados** |
| **Emprego formal (CAGED)** | OK | 2015–2025 (saldo mensal) |
| **Emprego formal (RAIS/estoque)** | FRACO | Gaps extensos, sem remuneração |
| SCR/BACEN (crédito por CNAE/porte) | AUSENTE | Depende de acesso formal (Prof. Magno) |
| SIOF estaduais (CE/PE/RN) | AUSENTE | Ainda não implementado |
| Proxy informalidade | AUSENTE | Previsto como 3o momento |
| Exportação/importação NE | AUSENTE | Previsto na tese original |

---

## 5. Problemas Identificados nos Logs

1. **CAGED Antigo jan/2015 e mar/2015:** `Corrupt input data` ao extrair `.7z` do FTP — dados de 2 meses perdidos (problema recorrente nas 4 execuções)
2. **Portal da Transparência sem API Key:** warning em todas as execuções — transferências BPC/Bolsa Família não foram coletadas
3. **Múltiplas execuções da pipeline** (4 logs): indica retrabalho — a última execução coletou apenas o módulo `caged_rais`

---

## 6. Resumo Executivo e Recomendações

### Pontos fortes
- Pipeline bem estruturada e modular (raw/processed por UF)
- BACEN, SICONFI e CAGED têm cobertura sólida e consistente
- Bimestralização do CAGED concluída sem gaps
- Metadados de coleta presentes

### Pontos críticos (ação necessária)

| Prioridade | Problema | Impacto | Ação sugerida |
|---|---|---|---|
| **ALTA** | RAIS com gaps severos e `remuneracao_media` vazia | Emprego é a variável dependente principal | Recolher RAIS via fonte alternativa (Base dos Dados/IBGE) ou usar CAGED como proxy de estoque via acumulação |
| **ALTA** | CAGED Antigo jan/2015 ausente | 1o bimestre de 2015 incompleto | Tentar download manual do `.7z` ou usar fonte alternativa para esse mês |
| **MEDIA** | BPC e Bolsa Família não coletados | Variáveis instrumentais do modelo revisado | Configurar API Key do Portal da Transparência ou usar SAGI/MDS |
| **MEDIA** | Exportação/importação NE ausente | Variável de controle prevista na tese | Coletar via MDIC/Comex Stat |
| **MEDIA** | SCR/BACEN não disponível | Crédito por CNAE/porte — diferencial da tese | Depende de solicitação institucional |
| **BAIXA** | SIOF estaduais (CE/PE/RN) ausentes | Transferências estaduais como controle | Sondar portais estaduais |
| **BAIXA** | DCA vai só até 2024 (falta 2025) | Gap de 1 ano no investimento público | Será publicado pelo SICONFI em meados de 2026 |

---

*Relatório gerado em 2026-03-04 com base na análise automatizada dos arquivos do projeto.*

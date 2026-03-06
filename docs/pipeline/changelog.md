# Changelog — Projeto de Doutorado Bruno Cardoso (DESP/UFC)

## [2026-03-06] Camada de Auditoria de Qualidade e Atualização da Documentação

### Mudança Central

O pipeline passou a tratar a qualidade dos dados públicos como parte explícita da arquitetura do projeto, e não apenas como verificação manual posterior.

### Evolução Posterior no Mesmo Dia

- Corrigida a agregação de `RAIS` para usar explicitamente `vinculo_ativo` no cálculo de estoque.
- Adicionados manifestos de status para `CAGED` antigo, `CAGED` novo e `RAIS`, melhorando rastreabilidade de arquivos ausentes ou corrompidos.
- A coleta de `transferencias` passou a usar whitelist auditável de `cod_conta`, substituindo filtro puramente textual.
- `Bolsa Família` passou a ter coleta persistida por UF quando há API key, com fallback automático para URLs de download quando não há.
- `pipeline/transform/preparacao_modelo.py` passou a gerar a camada `model_ready`, incluindo:
  - `matriz_regras_modelo.csv`
  - `resultado_primario_bimestral.csv`
  - `dcl_bimestral.csv`
  - `investimento_publico_bimestral.csv`
  - `transferencias_bimestrais.csv`
  - `rais_bimestral.csv`
  - `painel_tese_bimestral.csv`
- `pipeline/run.py` passou a suportar os modos `--etl`, `--preparar-modelo`, `--auditar` e `--full`.
- A auditoria foi expandida para cobrir fontes assistenciais, execuções estaduais e o painel final da tese.

### Alterações Implementadas

- Adicionado o módulo `pipeline/quality.py` para auditoria automatizada dos dados.
- Criado o diretório lógico `dados_nordeste/quality/` para armazenar artefatos de qualidade.
- Integrado o módulo `auditoria_qualidade` ao orquestrador `pipeline.run`.
- Atualizada a documentação operacional da pipeline para refletir o entrypoint real (`python3 -m pipeline.run`) e o fluxo completo `extract -> transform -> preparacao_modelo -> quality`.

### Artefatos Gerados

A auditoria agora produz:

- `quality_report.json`
- `quality_summary.csv`
- `quality_report.md`

### Checagens Automatizadas

- existência do arquivo
- cobertura temporal
- cobertura territorial por UF
- nulos totais
- nulos em colunas críticas
- duplicidade em chaves esperadas

### Achados Relevantes da Primeira Execução

- `RAIS`: anos esperados ausentes (`2019`, `2020`) e fragilidade em `remuneracao_media`
- `SICONFI DCA`: ausência de `2025`, coerente com a defasagem de publicação

### Impacto no Projeto

Essa mudança reforça a premissa metodológica de que, em dados públicos, a coleta também precisa funcionar como camada de auditoria. Isso melhora a rastreabilidade da tese e prepara o projeto para uma etapa posterior de integração analítica mais robusta.

## [2025-02-24] Redirecionamento da Tese — Reunião com orientadores

### Mudança Central

A tese deixa de focar exclusivamente na relação **crédito → IBCR-NE** e incorpora **emprego como variável dependente principal**, respondendo à pergunta-chave:

> "Se dermos R$ 1 bilhão em crédito, quantos mil empregos são gerados?"

### Estrutura Revisada do Modelo

- **Variável dependente**: Volume de empregos (formais, com proxy para informais)
- **Variáveis explicativas principais**:
  - Crédito PF (famílias) — pelo lado da oferta
  - Crédito PJ (empresas) — pelo lado da demanda (com cautela: fragilidade maior aqui)
- **Controles / variáveis instrumentais**:
  - IBCR-NE (mantido, agora como controle)
  - Transferências federais: BPC e Bolsa Família (API disponível)
  - Transferências estaduais: SIOF-CE, SIOF-PE, SIOF-RN (incerteza sobre disponibilidade)
  - Investimento público no longo prazo (melhoria de ambiência econômica)
- **Proxy para informalidade** (terceiro momento / possível artigo separado)

### Fontes de Dados e APIs — Sequência de Extração

**Primeiro momento (prioritário):**
- SCR/BACEN: séries de crédito por UF, CNAE, modalidade, porte — acesso mediante solicitação formal (Prof. Magno viabiliza)
- BACEN/SGS: séries de crédito PF e PJ por estado (já em uso)

**Segundo momento:**
- CAGED ou RAIS via API — série de empregos formais para os 9 estados do NE

**Terceiro momento:**
- Proxy de informalidade (complexidade de tese independente)
- Controles via IPEADATA API

### Produto Tecnológico — Separação Estratégica

O Prof. Paulo Matos estabeleceu uma divisão importante:

| Dimensão | Propósito | Conteúdo |
|---|---|---|
| Tese acadêmica | Publicação, defesa | Artigo com aplicação exemplificativa, comparação BNB vs. BACEN, manual da ferramenta |
| "Canhão" (produto real) | Ferramenta de gestão/predição | Camada agnóstica, poder preditivo, uso pelo BNB, secretarias, gestores públicos |

A lógica é "livrar a ferramenta" da tese — o produto tecnológico não precisa estar completamente maduro na defesa, mas precisa ter uma aplicação demonstrável e documentada.

### Pontos de Atenção Metodológica (Cássio)

- **Variável não observável**: informalidade como proxy — escolha metodológica precisa ser justificada
- **Dado subjetivo**: empregos gerados em projetos específicos — viés de mensuração
- **Viés de ineditismo**: explorar o que ainda não foi feito na literatura regional
- **Camada agnóstica**: a ferramenta deve funcionar independente da fonte de dados ou modelo subjacente

### Próximos Passos Concretos

1. Solicitar acesso ao SCR via Prof. Magno (dado disponível mediante cadastro)
2. Mapear API do CAGED/RAIS para série de empregos formais dos 9 estados
3. Mapear API de transferências federais (BPC + Bolsa Família — SICONFI/STN ou Portal da Transparência)
4. Sondar BNB sobre interesse nas ferramentas de gestão (abertura institucional)
5. Definir escopo do artigo acadêmico — aplicação exemplificativa com dados do BACEN vs. BNB
6. Revisar apresentação incorporando emprego como variável central

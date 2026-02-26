# Changelog — Projeto de Doutorado Bruno Cardoso (DESP/UFC)

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

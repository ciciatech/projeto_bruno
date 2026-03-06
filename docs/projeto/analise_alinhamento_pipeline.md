# Análise de Alinhamento do Projeto e do Pipeline

## 1. Objetivo central do projeto

Pelo material em `docs/projeto`, o projeto deixou de ter como foco principal apenas o crescimento econômico agregado e passou a buscar uma resposta causal e operacionalmente útil para política pública: estimar quanto emprego formal é gerado no Nordeste quando há expansão do crédito, distinguindo crédito para pessoa física e pessoa jurídica, no período de 2015 a 2025.

Isso implica que a arquitetura de dados do projeto precisa entregar, no mínimo, cinco capacidades:

1. Cobertura temporal íntegra de 2015 a 2025.
2. Variável dependente de emprego consistente, preferencialmente em mais de uma ótica.
3. Variáveis explicativas e controles harmonizados na mesma granularidade temporal.
4. Base analítica final pronta para cruzamentos por UF, tempo e, quando possível, setor.
5. Rastreabilidade metodológica suficiente para sustentar uma tese robusta e um produto tecnológico confiável.

Hoje o projeto já tem boa estrutura de coleta e documentação, mas ainda não fecha esse ciclo ponta a ponta.

---

## 2. Diagnóstico executivo

### Avaliação geral

| Eixo | Situação atual | Avaliação |
| --- | --- | --- |
| Propósito da tese | Bem documentado e relativamente claro | Forte |
| Extração das fontes centrais | BACEN, SICONFI e CAGED estão bem encaminhados | Forte |
| Variável dependente principal | CAGED está forte; RAIS ainda é frágil | Parcial |
| ETL por fonte | Existe e organiza bem os dados | Bom |
| Harmonização completa para modelo | Parcial, ainda incompleta | Parcial |
| Cruzamentos analíticos finais | Ainda não consolidados em uma base mestra | Fraco |
| Robustez metodológica para wavelet | Ainda insuficiente para execução final | Parcial |
| Robustez para produto tecnológico | Boa base inicial, mas depende de camada analítica final | Parcial |

### Síntese

O projeto está mais maduro na camada de coleta do que na camada de integração analítica. Em outras palavras: há bons blocos de dados, mas ainda não existe um dataset mestre totalmente alinhado ao modelo econométrico final nem aos cruzamentos estratégicos que respondem diretamente à pergunta da tese.

---

## 3. Alinhamento entre propósito e dados

### 3.1 O que o projeto precisa responder

A pergunta substantiva do projeto hoje é algo próximo de:

> Dado um aumento do crédito no Nordeste, qual é o efeito sobre o emprego formal, controlando por atividade econômica, condições monetárias, situação fiscal e transferências públicas?

Para responder isso com robustez, a base deve articular:

- emprego como variável dependente principal;
- crédito PF e PJ como variáveis explicativas centrais;
- controles macro nacionais;
- controles fiscais estaduais;
- transferências públicas;
- possibilidade de heterogeneidade por estado e, idealmente, por setor.

### 3.2 O que já está alinhado

- Crédito e macro nacional estão bem cobertos.
- O emprego por fluxo já está muito mais alinhado ao objetivo atual do que ao objetivo antigo.
- A presença de SICONFI, transferências e execução orçamentária amplia a chance de isolar melhor o efeito do crédito.
- A documentação já reconhece corretamente que o padrão-alvo da tese é bimestral.

### 3.3 O que ainda não está totalmente alinhado

- Parte da documentação operacional ainda carrega a formulação antiga do projeto, focada em crescimento agregado.
- A base final ainda não está montada ao redor da variável dependente nova como centro do sistema.
- Os cruzamentos mais valiosos para a tese, especialmente crédito x emprego por estado e crédito x emprego por setor, ainda não foram materializados como tabelas analíticas finais.

---

## 4. Avaliação da extração

## 4.1 BACEN

É a parte mais madura do pipeline.

Pontos fortes:

- séries centrais de crédito, IBCR-NE, SELIC, IPCA e IBC-Br já aparecem no pipeline;
- cobertura temporal longa e contínua;
- formato wide facilita preparação posterior;
- há base suficiente para deflacionamento e harmonização.

Pontos de atenção:

- o conjunto BACEN já atende bem ao projeto, então o problema aqui não é coleta, e sim integração com as demais fontes;
- há mistura entre indicadores que são diretamente centrais ao modelo e outros que entram como apoio, o que ainda pede uma camada de curadoria analítica.

Conclusão:

O eixo BACEN está aderente ao propósito da tese e não é o gargalo principal.

## 4.2 CAGED e RAIS

Esse eixo é o coração do projeto atual.

Pontos fortes:

- o pipeline já incorporou a mudança de foco para emprego;
- há coleta de CAGED antigo e novo;
- a continuidade 2015-2025 do fluxo de emprego está conceitualmente bem desenhada;
- há agregações por UF, setor e perfil, o que é muito valioso.

Fragilidades:

- a estratégia principal de coleta ainda depende fortemente do FTP do MTE, que é menos robusto do que a rota documentada via BigQuery/Base dos Dados;
- os próprios relatórios do projeto já registram problemas de arquivos corrompidos em partes do CAGED antigo e sobretudo da RAIS;
- a RAIS, que deveria fortalecer o estoque de emprego, ainda está incompleta e com baixa confiabilidade para algumas variáveis;
- a última metadata de execução indica que a coleta mais recente rodou apenas `caged_rais`, o que sugere que o pipeline completo ainda não está sendo reexecutado de forma integrada.

Conclusão:

O alinhamento conceitual é forte, mas a robustez operacional ainda é parcial. Para uma tese, isso é crítico porque a variável dependente precisa ser a parte mais confiável do sistema, e hoje ainda não é.

## 4.3 SICONFI

É o segundo eixo mais relevante para isolar o efeito do crédito.

Pontos fortes:

- cobertura ampla por UF e por ano;
- boa aderência aos controles fiscais previstos;
- o ETL já extrai subconjuntos úteis como resultado primário, dívida e investimento.

Fragilidades estruturais:

- a coleta em `pipeline/extract/siconfi.py` ainda está mais genérica do que a parametrização validada na documentação técnica do projeto;
- faltam alguns filtros específicos por esfera, poder e anexo na chamada de coleta, o que pode ampliar demais o universo bruto e exigir limpeza posterior desnecessária;
- o pipeline ainda não transforma automaticamente RGF e DCA para a granularidade bimestral final.

Conclusão:

SICONFI tem alta aderência substantiva, mas a rota atual está mais próxima de uma coleta ampla seguida de filtragem do que de uma extração analítica precisa.

## 4.4 Transferências e renda pública

Esse é hoje um dos pontos menos robustos do projeto.

Pontos positivos:

- há extração de transferências constitucionais a partir do RREO;
- a documentação reconhece corretamente a importância de Bolsa Família e BPC como controles.

Fragilidades:

- o módulo de transferências usa filtro por palavras-chave sobre contas do RREO, o que é útil como exploração, mas menos robusto como variável final de modelo;
- o módulo de Bolsa Família não faz coleta consolidada dos dados analíticos, apenas gera URLs de download;
- BPC segue ausente da esteira principal.

Conclusão:

O projeto reconhece corretamente a importância dessas variáveis, mas ainda não possui uma camada de extração madura o suficiente para tratá-las como controles finais robustos.

## 4.5 Execução orçamentária estadual

Pontos fortes:

- é um diferencial potencial do projeto;
- pode enriquecer bastante a interpretação de heterogeneidade estadual.

Limitações:

- cobertura ainda incompleta entre estados;
- schemas diferentes entre fontes estaduais;
- ainda parece mais um eixo complementar do que parte consolidada da base econométrica principal.

Conclusão:

É um ativo estratégico para diferenciação do projeto, mas não deve ser tratado como pré-requisito para fechar a base principal da tese.

---

## 5. Avaliação do ETL

## 5.1 O que o ETL já faz bem

O ETL em `pipeline/transform/etl.py` cumpre razoavelmente bem a função de:

- padronizar saídas por fonte;
- separar arquivos por UF e por tema;
- resumir contas fiscais importantes;
- organizar dados para consumo em dashboard e exploração.

Isso é importante porque cria uma camada intermediária legível e reutilizável.

## 5.2 O que o ETL ainda não faz

O ETL ainda não entrega a principal peça analítica do projeto:

- não existe uma tabela mestra final unificando `UF x tempo x variáveis do modelo`;
- não há construção explícita de painel final pronto para econometria;
- não há criação de uma versão "gold" do dataset da tese;
- não há validação automática de integridade temporal, duplicidade, missingness crítica e coerência entre fontes;
- os cruzamentos ainda estão mais implícitos na estrutura de pastas do que explícitos em uma base final.

Em termos práticos, o ETL atual organiza dados, mas ainda não "fecha a modelagem".

---

## 6. Avaliação da preparação para o modelo

O arquivo `pipeline/transform/preparacao_modelo.py` é importante porque mostra que o projeto já começou a caminhar para o formato exigido pelo wavelet.

### Pontos positivos

- há função de deflacionamento;
- há regra de agregação mensal para bimestral;
- há harmonização explícita do BACEN;
- há harmonização do CAGED para bimestre;
- já existe uma função genérica para interpolar anual para bimestral.

### Limitações críticas

- o orquestrador dessa etapa ainda não harmoniza SICONFI, transferências e RAIS até o formato final;
- a função de interpolação existe, mas ainda não está integrada ao fluxo principal da preparação;
- não há geração de base única final pronta para wavelet;
- não há camada de seleção final das variáveis do modelo com nomes padronizados, dicionário fixo e esquema estável.

Conclusão:

A preparação metodológica foi iniciada, mas ainda está em meia etapa. Hoje ela é suficiente para prototipagem, não para sustentar a execução final do modelo da tese com máxima robustez.

---

## 7. Avaliação dos cruzamentos de dados

## 7.1 Cruzamentos já viáveis

Com o que já existe, o projeto pode sustentar:

- crédito x emprego por UF e bimestre;
- crédito x resultado primário por UF;
- crédito x dívida e investimento público por UF, desde que a harmonização temporal seja concluída;
- comparações entre estados do Nordeste com controles macro nacionais comuns.

## 7.2 Cruzamentos ainda não materializados

Os cruzamentos mais importantes para o objetivo final ainda não estão estruturados como produtos de dados:

- painel final `uf x ano_bim x bimestre`;
- painel expandido com emprego, crédito, fiscal e transferências no mesmo registro;
- painel setorial `uf x setor x tempo`, que seria especialmente forte para a tese;
- camada de indicadores derivados, como crédito real por trabalhador, elasticidade observada preliminar, participação relativa por estado e intensidade fiscal.

## 7.3 Gargalo central dos cruzamentos

O principal gargalo não é a ausência total de dados, e sim a ausência de uma camada explícita de integração analítica. O projeto tem várias peças úteis, mas elas ainda não foram transformadas em um sistema coerente de evidência.

---

## 8. Grau de robustez atual

### O que já é robusto

- documentação conceitual do objetivo;
- organização geral do pipeline;
- coleta de BACEN;
- coleta fiscal ampla via SICONFI;
- estrutura de pastas raw e processed;
- logging e metadados;
- incorporação de CAGED antigo e novo.

### O que ainda não é robusto o suficiente

- estratégia principal de coleta da variável dependente por FTP;
- coleta efetiva de Bolsa Família e BPC;
- isolamento final e preciso das transferências que entram no modelo;
- harmonização completa de todas as variáveis para bimestre;
- base analítica única e reproduzível;
- suíte de testes de qualidade de dados orientada ao modelo.

### Julgamento geral

O projeto já é promissor e relativamente bem estruturado, mas ainda não está robusto no sentido acadêmico forte. Ele está robusto como pipeline de coleta modular em evolução, porém ainda não como infraestrutura fechada para inferência econométrica e produto tecnológico de alta confiabilidade.

---

## 9. Principais desalinhamentos entre projeto e implementação

### 1. O objetivo mudou mais rápido do que a arquitetura final

A tese migrou para emprego como variável dependente principal, mas a arquitetura ainda carrega traços do desenho antigo focado em crescimento agregado. A coleta já reagiu parcialmente a isso; a integração analítica, ainda não.

### 2. O pipeline é forte em extração, mas fraco em consolidação

O sistema coleta várias fontes e salva bem os arquivos, mas ainda não produz automaticamente a base final da tese.

### 3. A camada de cruzamentos está subdesenvolvida

Ainda falta uma modelagem explícita de relações entre fontes, chaves, granularidades e regras de agregação.

### 4. A robustez da variável dependente ainda não está no nível ideal

Como a pergunta central depende de emprego, as fragilidades de RAIS e parte do CAGED antigo têm peso desproporcional sobre a solidez do projeto.

### 5. A preparação para wavelet ainda não cobre todas as fontes

Deflacionamento e harmonização existem como ideia implementada parcialmente, mas não fecharam o pipeline econométrico completo.

---

## 10. Arquitetura recomendada para tornar o projeto robusto

## 10.1 Estrutura de camadas

Recomenda-se formalizar o pipeline em quatro camadas:

### Camada 1: Raw

Dados brutos imutáveis por fonte, período e versão.

### Camada 2: Staging

Dados padronizados por fonte, com schema estável, tipos corrigidos, chaves explícitas e dicionário validado.

### Camada 3: Analytical

Tabelas prontas para cruzamento:

- painel por UF e bimestre;
- painel por UF, setor e bimestre;
- painel anual de estoque de emprego;
- tabelas auxiliares de metadados e qualidade.

### Camada 4: Model-ready

Base final com seleção definitiva das variáveis da tese, pronta para wavelet, VAR, regressões auxiliares e dashboards analíticos.

## 10.2 Chave analítica principal

A chave principal da tese deveria ser explicitamente:

- `uf`
- `ano_bim`
- `bimestre`

E, na versão setorial:

- `uf`
- `ano_bim`
- `bimestre`
- `divisao_cnae` ou outra unidade setorial estável

## 10.3 Contratos de dados

Cada variável crítica do modelo deveria ter:

- definição econômica;
- fonte oficial;
- unidade de medida;
- frequência original;
- regra de agregação para bimestre;
- regra de deflacionamento;
- regra de imputação ou interpolação, quando houver;
- teste mínimo de qualidade.

---

## 11. Prioridades objetivas

### Prioridade 1

Construir a base analítica final da tese, unificando BACEN, CAGED, SICONFI e transferências em um painel bimestral por UF.

### Prioridade 2

Trocar a estratégia principal de RAIS/CAGED antigo para uma rota mais robusta, preferencialmente BigQuery/Base dos Dados, deixando FTP como contingência.

### Prioridade 3

Transformar Bolsa Família e BPC em datasets realmente integrados, e não apenas em links ou coleta parcial.

### Prioridade 4

Completar a harmonização temporal de DCL, investimento público, RAIS e demais variáveis ainda fora do padrão bimestral final.

### Prioridade 5

Criar testes automáticos de qualidade orientados ao modelo:

- cobertura temporal esperada;
- contagem de UFs por período;
- missing por variável crítica;
- duplicidade por chave;
- consistência de unidade e sinal;
- comparação entre execuções.

### Prioridade 6

Padronizar definitivamente a narrativa do projeto inteiro em torno do objetivo atual: impacto do crédito sobre o emprego, com IBCR-NE e demais variáveis como controles.

---

## 12. Conclusão final

O projeto já possui base suficiente para evoluir para algo muito forte. A direção metodológica está correta, as fontes mais importantes já foram identificadas, a coleta central avançou bem e o potencial de originalidade é real, principalmente nas combinações entre crédito, emprego, fiscalidade e heterogeneidade regional.

O ponto decisivo agora é sair de um projeto "rico em arquivos" para um projeto "rico em integração analítica". A extração já está relativamente madura; o próximo salto é transformar tudo isso em uma base final reproduzível, auditável e diretamente conectada à pergunta da tese.

Se esse passo for executado com disciplina, o projeto fica bem posicionado para ser robusto em três frentes ao mesmo tempo:

- academicamente, para sustentar a tese;
- metodologicamente, para suportar wavelet e análises complementares;
- tecnologicamente, para alimentar um produto inteligente realmente confiável.

# Metodologia · Composição do investimento total no Ceará

> Consolidação das decisões metodológicas do Prof. Paulo Matos (orientador, CAEN/UFC)
> a partir de 3 transcrições de áudios enviados em abr/2026.

## Visão geral

A tese trabalha com o **investimento agregado no Ceará** decomposto em
**4 esferas + residual privado**:

```
Investimento total CE = Estadual + Municipal + Federal + Privado (residual)
```

Cada esfera tem fonte e método próprios. O privado é uma "conta de
chegada" — calculado por exclusão.

---

## 1. Investimento Estadual (SIOF-CE)

**Fonte**: SIOF (Sistema Integrado de Orçamento e Finanças do CE) —
documentos PDF anuais publicados pela SEPLAG-CE.

**Granularidade nativa**: região × ano (14 regiões SEPLAG/IPECE).

**Variável**: empenhado em obras e equipamentos (categoria 4 do SIOF).

**Tratamento no painel**: replicado em todos os meses do ano (anual → mensal
por replicação, não por divisão).

**Status no projeto**: ⚠️ **parcial — apenas 2026 detalhado por região**.

> ### ⚠️ Lacuna identificada (2026-05-01)
>
> A SEPLAG-CE só publica o detalhamento por região no SIOF a partir do
> exercício **2026**. Os anos 2015–2025 estão disponíveis apenas no nível
> "secretaria" (8 dígitos), sem desagregação pelas 14 macrorregiões.
>
> Verificação: `dados_nordeste/raw/execucao_orcamentaria/ce/siof_obras_consolidado.csv`
> contém códigos de 2 dígitos (regiões) **somente** para `ano == 2026`.
> Em 2015–2025 só aparecem códigos de 8 dígitos (secretarias).
>
> **Impacto na tese**: o exercício causal regional (emprego × investimento
> estadual em obras nas 14 regiões CE) **fica inviabilizado para 2015–2025**
> pela ausência de dado regional do SIOF. Pendente decisão do Prof. Paulo.
>
> **Caminhos não-destrutivos**:
> 1. **Requisição LAI à SEPLAG-CE** pelos dados regionais 2015-2025 (mais
>    correto cientificamente, depende de prazo institucional).
> 2. **Contato direto IPECE/SEPLAG via vínculo UFC do Bruno**.
> 3. **Pivotar variável de impacto** para `invest_municipal_siconfi`, que tem
>    180/184 municípios CE cobertos em 2015–2025 (R$ 28,97 bi total) — mantém
>    granularidade regional mas reformula a tese para "investimento *público
>    municipal*" em vez de "estadual".
> 4. **Restringir escopo** a 2026 (não recomendado — pouco dado para causalidade).
>
> **Caminhos a evitar**: ratear o total estadual SIOF entre as 14 regiões
> usando chave (população, PIB, etc.) — vira invenção de dado e contamina
> a inferência causal.
>
> **Mitigação UX no dashboard** (commit `feat(frontend): overlay SIOF
> sem-dado-regional`): quando o filtro de período da Tela 1 cobre anos sem
> dado regional, o mapa coroplético renderiza overlay editorial com botão
> "Ver 2026" em vez de pintar todas as regiões com a cor zero (que dá
> falsa sensação de homogeneidade).

**Output**: `dados_nordeste/processed/execucao_orcamentaria/ce/siof_obras_regiao.csv`.

---

## 2. Investimento Municipal (SICONFI)

**Fonte**: SICONFI / Tesouro Nacional, RREO Anexo 01.

**Endpoint**: `apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo` — API pública,
JSON.

**Granularidade nativa**: município × bimestre (acumulado-no-ano).

**Filtro adotado**:
- `anexo == "RREO-Anexo 01"` (Balanço Orçamentário)
- `cod_conta == "Investimentos"` (categoria econômica direta)
- `coluna == "DESPESAS EMPENHADAS ATÉ O BIMESTRE (f)"`

**Tratamento anti-erro** (citação direta do áudio do Prof. Paulo):

> "Tem umas manhas. Às vezes as prefeituras divulgam os dados de maneira
> equivocada. Em vez de colocar o dado acumulado, às vezes ela coloca o dado
> corrente. A gente tem que criar uns truquezinhos, uns filtrozinhos para
> perceber quando eles erram."

**Implementação**: o coletor compara o valor acumulado do bimestre N+1 com
o do bimestre N. Se `valor_b{n+1} < valor_b{n} × 0.95` (mesmo ano, mesma
prefeitura), marca como `suspeito_relato_corrente` e registra em
`dados_nordeste/quality/invest_municipal_siconfi_audit.csv`. O dado **não é
descartado** — apenas sinalizado para revisão.

**Conversão bimestre → mensal**:
```
fluxo_bimestre_n = valor_acum_b{n} - valor_acum_b{n-1}
mes_2n-1 = mes_2n = fluxo_bimestre_n / 2
```

**Status**: ✅ coletor implementado em `pipeline/extract/invest_municipal_siconfi.py`
e coleta concluída (T01): 19.896 registros, 180 municípios CE, R$ 28,97 bi 2015-2025.
⚠️ Pendência (T34): a coluna coletada é "DESPESAS EMPENHADAS ATÉ O BIMESTRE", mas o
gabarito do Prof. Paulo usa valores **PAGOS** (divergência +7% a +37% a.a.) — aguarda
confirmação dele; se "pago", recoleta SICONFI ~13k requests.

---

## 3. Investimento Federal (RREO União)

**Fonte**: Portal da Transparência (CSV bulk de despesas-execução) +
RREO da União.

**Metodologia (3 componentes)**:

### 3.1 Aplicações Diretas no CE
> "Aí o que ocorre: a gente vai no RREO da União, aplicações diretas,
> vê tudo o que foi gasto no Ceará."

Filtros:
- Código Grupo de Despesa = **4** (Investimentos)
- Código Modalidade da Despesa = **90** (Aplicações Diretas)
- Localização = CE (UF 23)

### 3.2 Rateio NE → CE
> "No RREO da União, quando uma obra transpassa um estado, abrange dois
> estados ou mais, ela não identifica o estado, mas identifica a região.
> O Estado do Ceará tem 14,5% do PIB da região. Aí a gente assume que todo
> investimento que o Lula faz na região Nordeste, mas não identifica qual é
> o estado, 14,5% desse investimento foi para o Ceará."

```
ne_rateado_ce = invest_NE_indefinido × 0.145
```

> **Em revisão (jun/2026)**: a planilha bimestral do Prof. Paulo usa share
> **regional de 15,4%** para o CE + vetor de pesos regionais embutido no
> cabeçalho do bloco — confirmação pendente (ver tasks T18 e
> `docs/estudo-viabilidade-painel-bimestral.md`). As citações de áudio acima
> ficam como registro histórico de abr/2026.

### 3.3 Rateio Nacional → CE
> "Às vezes corta estados de mais de uma região, aí o governo federal não
> identifica é nada. Aí a gente pega esse investimento da União que não vai
> para canto nenhum e assume que 2,2% dele veio para o Ceará, porque é o
> PIB do Ceará na União."

```
nacional_rateado_ce = invest_nacional_indefinido × 0.022
```

### 3.4 Total federal
```
invest_fed_total = direto_ce + ne_rateado_ce + nacional_rateado_ce
```

**Atenção (citação)**:
> "Não são contabilizados repasses ao governo estadual para evitar a
> contagem duplicada, já que esses valores já constam no RREO estadual."

**Status**: ✅ implementado em `pipeline/extract/invest_federal.py`.
Output: `invest_federal_ce_mensal.csv` com colunas `direto_ce`,
`ne_rateado`, `nacional_rateado`, `total`.

---

## 4. Investimento Privado (residual)

**Fonte**: cálculo por exclusão.

```
inv_privado = inv_total_CE − inv_estadual − inv_municipal − inv_federal
```

### 4.1 Estimação do total CE

> "O Ipea divulga no IpeaData o investimento total brasileiro, tanto fluxo
> como estoque, mensal até 2024 [...] Aí o que ocorre: a gente tem que
> assumir que o share desse investimento total no Brasil no Ceará segue o
> mesmo percentual que o share do PIB do Ceará no Brasil. O Ceará
> historicamente tem um PIB ali que, na média, é 2,2% do PIB nacional."

```
inv_total_CE = FBCF_Brasil_mensal × 0.022
```

**Fonte FBCF Brasil**: IpeaData série `BM12_FBKFM12` (FBCF mensal, indicador)
+ `SCN10_FBKFP10` (FBCF anual real, R$ 2010 mi).

**Hipótese explícita**: o share do PIB CE/BR (~2,2%) é estável no tempo e
aplica também ao investimento. É uma aproximação — não há base oficial
para FBCF estadual.

**Coletor**: `pipeline/extract/ipea_fbcf.py` ✅ implementado. Output:
`fbcf_brasil_mensal.csv`.

### 4.2 Base monetária

O áudio do Paulo (abr/2026) descreve o IpeaData como já entregando os valores em
**R$ presente de dezembro/2024**, e dez/24 foi a base aprovada à época.
Verificação no nosso coletor: o IpeaData disponibiliza tanto **R$ correntes**
quanto **R$ 2010**.

**Em revisão (jun/2026)**: a planilha bimestral do Prof. Paulo usa base
**dez/2025** (deflator 25b6 = 1,0 exato) — a **base de trabalho atual é dez/25**,
com dez/24 superseded salvo decisão contrária dele (confirmação formal pendente,
ver `docs/estudo-viabilidade-painel-bimestral.md` e tasks T18/T28).

Deflator implementado e validado (T28): `pipeline/transform/deflator.py` — IPCA
SGS 433 (cache offline `pipeline/data/ipca_433_cache.csv`), base **pinada e
parametrizável** (default `2025-12`; dez/24 pronto se o Paulo preferir).
Reproduz os 66 deflatores da planilha dele com erro máximo 2×10⁻¹⁵. O painel
bimestral (T27) já publica 20 colunas `*_real` nessa base.

No painel mensal e na composição do frontend ainda valem **valores nominais** com
**aviso visual** ("⚠ bases mistas — refinamento monetário pendente") até a
harmonização das 4 esferas (ver `tasks.md`, "Deflator IPCA do painel mensal").

### 4.3 Para 2025

> "Tem até que fazer uma previsão — é o que a gente tem feito para 2025; a
> última vez que eu olhei eles não tinham calculado ainda para 2025."

**Pendência**: implementar previsão univariada (ex: ARIMA simples ou
extrapolação por share) para o investimento total CE em 2025. Quando o
IpeaData publicar o dado oficial, substituir.

---

## 5. Por que essa "engenharia"?

Citação final do áudio:

> "Engenharia danada, mas cara, é o que a gente tem usado para a SEFAZ,
> porque de outra forma você não tem o dado."

Não existe dado oficial de FBCF estadual nem decomposição completa por
esfera. A metodologia aqui é a **convenção de trabalho aprovada pelo
orientador** — deve ser citada explicitamente em qualquer publicação:

```
Investimento total estimado para o Ceará via aplicação do share histórico
do PIB CE/Brasil (2,2%) sobre a série mensal de FBCF Brasil do IpeaData
(SCN10_FBKFP10). Investimento federal calculado via 3 componentes do RREO
União (aplicações diretas + rateio NE × 14,5% + rateio nacional × 2,2%).
Investimento privado computado por exclusão.
```

> **Em revisão (jun/2026)**: a planilha bimestral do Prof. Paulo usa share
> regional **15,4% CE** + vetor de pesos regionais no lugar do rateio NE ×
> 14,5% — confirmação pendente (ver tasks T18 e o estudo de viabilidade).
> Atualizar este bloco citável quando a confirmação chegar.

---

## 6. Status de implementação

| Esfera | Coletor | Painel | Status |
|--------|---------|--------|--------|
| Estadual (SIOF) | siof.py | `siof_emp` (anual replicado) | ✅ |
| Federal (RREO) | invest_federal.py | `if_total`, `if_direto`, `if_ne`, `if_nac` | ✅ (share 14,5% em revisão → 15,4% regional) |
| Municipal (SICONFI) | invest_municipal_siconfi.py | `invest_mun_valor` (mapeado) | ✅ concluído (T01 — 19.896 registros; ⚠️ EMPENHADO vs PAGO pendente, T34) |
| Privado residual | (cálculo no frontend) | derivado | ✅ derivado nas 4 esferas (harmonização monetária do mensal pendente) |
| Total CE estimado | ipea_fbcf.py | `inv_tot` | ✅ (deflator implementado — T28, base dez/25 pendente de confirmação) |
| Share PIB CE/BR | pib_shares.py | `share` | ✅ |

---

## Referências internas

- `pipeline/extract/ipea_fbcf.py` — coletor FBCF
- `pipeline/extract/invest_federal.py` — coletor RREO + Portal Transp.
- `pipeline/extract/invest_municipal_siconfi.py` — coletor SICONFI
- `pipeline/extract/siof.py` — parser SIOF-CE
- `pipeline/pib_shares.py` — share CE/BR e CE/NE
- `frontend/src/screens/Investimento.tsx` — visualização da composição

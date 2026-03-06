# Pipeline de Coleta de Dados Públicos - Tese DESP/UFC

## Impactos do Crédito e do Emprego no Nordeste

Pipeline Python para coleta, organização, preparação e auditoria de dados públicos usados na tese de doutorado de Bruno Cardoso Costa.

O objetivo atual do projeto é sustentar a análise do efeito do crédito sobre o emprego formal no Nordeste entre 2015 e 2025, com controles macroeconômicos, fiscais e de transferências públicas.

## Visão Geral

O pipeline está organizado em quatro camadas:

1. `extract`: coleta dados brutos de APIs, FTPs e portais públicos.
2. `transform`: padroniza e organiza os dados em saídas por fonte, UF e tema.
3. `preparacao_modelo`: aplica deflacionamento e harmonização temporal para o modelo.
4. `quality`: audita cobertura, nulos, duplicidade e consistência mínima dos dados coletados.

## Módulos Disponíveis

| Módulo | Fonte | Saída principal |
|--------|-------|-----------------|
| `bacen` | BACEN-SGS | crédito, IBCR-NE, IBC-Br, SELIC, IPCA, inadimplência |
| `siconfi_rreo` | SICONFI/STN | receitas, despesas e resultado primário |
| `siconfi_rgf` | SICONFI/STN | dívida e gestão fiscal |
| `siconfi_dca` | SICONFI/STN | balanço patrimonial e investimento público |
| `transferencias` | SICONFI/RREO | transferências por whitelist auditável de rubricas |
| `bolsa_familia` | Portal da Transparência / dados abertos | coleta mensal por UF quando há API key; fallback por URLs |
| `caged_rais` | FTP MTE/PDET | CAGED antigo, Novo CAGED e RAIS |
| `auditoria_qualidade` | Camada interna do projeto | relatórios de qualidade dos dados |

## Instalação

```bash
pip install requests pandas tqdm openpyxl pyarrow py7zr
```

## Execução

### 1. Coleta rápida

```bash
python3 -m pipeline.run --apenas-bacen
```

### 2. Coleta principal

Executa os módulos padrão do orquestrador.

```bash
python3 -m pipeline.run
```

### 3. Coleta por módulos

```bash
python3 -m pipeline.run --modulos bacen siconfi_rreo transferencias
```

### 4. Coleta de emprego

```bash
python3 -m pipeline.run --modulos caged_rais
```

### 5. ETL

```bash
python3 -m pipeline.transform.etl
```

### 6. Preparação para o modelo

```bash
python3 -m pipeline.transform.preparacao_modelo
```

### 7. Auditoria de qualidade

```bash
# Auditoria isolada
python3 -m pipeline.quality

# Auditoria via orquestrador
python3 -m pipeline.run --modulos auditoria_qualidade
```

### 8. Bolsa Família com chave de API

```bash
python3 -m pipeline.run --modulos bolsa_familia --api-key SUA_CHAVE
```

### 9. Fluxo completo

```bash
python3 -m pipeline.run --full
```

## Estrutura de Saída

```text
dados_nordeste/
├── raw/                          # Dados brutos por fonte
├── processed/                    # Dados tratados para análise e dashboard
│   └── model_ready/              # Saídas finais para modelagem
├── quality/                      # Relatórios de qualidade e cobertura
├── logs/                         # Logs de execução
└── metadata_coleta.json          # Metadados da última execução do run.py
```

### Convenção de diretórios

- `raw/<fonte>/<escopo>/arquivo.csv`
- `processed/<fonte>/<uf_ou_escopo>/arquivo.csv`
- `processed/model_ready/painel_tese_bimestral.csv`
- `processed/model_ready/matriz_regras_modelo.csv`
- `quality/quality_report.json`
- `quality/quality_summary.csv`
- `quality/quality_report.md`

## Auditoria de Qualidade

A auditoria existe porque, em dados públicos, a coleta não é apenas ingestão. Ela também é uma etapa de validação metodológica.

As checagens atuais incluem:

- existência do arquivo esperado
- contagem de registros e colunas
- percentual total de nulos
- nulos em colunas críticas
- duplicidade em chaves esperadas
- cobertura temporal
- cobertura territorial por UF

### Fontes auditadas atualmente

- `bacen_sgs_wide.csv`
- `caged_antigo_saldo_mensal.csv`
- `caged_saldo_mensal.csv`
- `rais_vinculos.csv`
- `siconfi_rreo_nordeste.csv`
- `siconfi_rgf_nordeste.csv`
- `siconfi_dca_nordeste.csv`
- `transferencias_constitucionais_nordeste.csv`
- `bolsa_familia_uf_mensal.csv`
- `painel_tese_bimestral.csv`
- `bacen_bimestral.csv`
- `caged_bimestral.csv`

### Uso analítico da auditoria

Essa camada ajuda a detectar:

- lacunas de período
- cobertura territorial incompleta
- mudanças de layout
- fragilidade em variáveis críticas
- problemas de consistência antes da modelagem

## Séries BACEN Coletadas

| Código | Descrição |
|--------|-----------|
| `25389` | IBCR Nordeste com ajuste sazonal |
| `14084` | Saldo de crédito PF - Nordeste |
| `14089` | Saldo de crédito PJ - Nordeste |
| `14079` | Saldo de crédito total - Nordeste |
| `4189` | SELIC mensal |
| `433` | IPCA mensal |
| `22109` | PIB trimestral em índice |
| `21084` | Inadimplência PF |
| `21085` | Inadimplência PJ |
| `20539` | Crédito PF Brasil |
| `20541` | Crédito PJ Brasil |
| `24364` | IBC-Br |

## Observações Importantes

- O `SICONFI` pode demorar bastante por conta de volume e rate limit.
- A coleta de `CAGED` e `RAIS` usa hoje a rota FTP do MTE/PDET.
- `RAIS` agora usa filtro explícito de `vinculo_ativo` na agregação de estoque.
- A camada `model_ready` gera um painel bimestral final em `processed/model_ready/painel_tese_bimestral.csv`.
- `RAIS` tem defasagem natural de publicação e apresenta fragilidades em alguns anos.
- `Bolsa Família` já possui esteira persistida por UF quando há API key; sem chave, a pipeline salva fallback de URLs para download.
- `BPC` continua pendente como controle assistencial separado.
- Os dados são salvos em CSV; a documentação antiga mencionava Parquet, mas o estado atual do pipeline é centrado em CSV.
- O script é idempotente no sentido operacional: uma nova execução sobrescreve os arquivos de saída.

## Limitações Conhecidas

- O painel final já existe, mas ainda depende da qualidade upstream das fontes mais frágeis.
- A harmonização bimestral já cobre o painel principal, porém parte da semântica temporal de fontes públicas ainda exige validação econométrica fina.
- A qualidade de `RAIS` continua sendo um dos principais pontos de atenção do projeto.

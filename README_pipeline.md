# Pipeline de Coleta de Dados Públicos - Tese DESP/UFC

## Impactos do Crédito no Crescimento Econômico do Nordeste

Pipeline Python para coleta automatizada de dados públicos de múltiplas fontes governamentais brasileiras.

## Fontes de Dados

| Módulo | Fonte | Dados |
|--------|-------|-------|
| `bacen` | BACEN-SGS | IBCR-NE, Crédito PF/PJ Nordeste, SELIC, IPCA, Inadimplência |
| `siconfi_rreo` | SICONFI/STN | Receitas, despesas, transferências recebidas (bimestral) |
| `siconfi_rgf` | SICONFI/STN | Dívida, operações de crédito, garantias (quadrimestral) |
| `siconfi_dca` | SICONFI/STN | Balanço patrimonial, demonstrações contábeis (anual) |
| `transferencias` | Tesouro Nacional | FPE, FPM, FUNDEB e outras transferências constitucionais |
| `bolsa_familia` | Portal da Transparência | URLs para download de pagamentos do Bolsa Família |

## Instalação

```bash
pip install requests pandas tqdm openpyxl pyarrow
```

## Uso

### Teste rápido (apenas BACEN)
```bash
python pipeline_coleta_dados_nordeste.py --apenas-bacen
```

### Coleta completa (exceto Bolsa Família)
```bash
python pipeline_coleta_dados_nordeste.py
```

### Módulos específicos
```bash
python pipeline_coleta_dados_nordeste.py --modulos bacen siconfi_rreo transferencias
```

### Com API Key do Portal da Transparência
```bash
# Via argumento
python pipeline_coleta_dados_nordeste.py --modulos bolsa_familia --api-key SUA_CHAVE

# Via variável de ambiente
export PORTAL_TRANSPARENCIA_API_KEY=SUA_CHAVE
python pipeline_coleta_dados_nordeste.py --modulos bolsa_familia
```

## Estrutura de Saída

```
dados_nordeste/
├── raw/                          # Dados brutos
│   ├── bacen_sgs_series.csv      # Séries BACEN (formato long)
│   ├── bacen_sgs_wide.csv        # Séries BACEN (formato wide/pivotado)
│   ├── siconfi_rreo_nordeste.csv
│   ├── siconfi_rgf_nordeste.csv
│   ├── siconfi_dca_nordeste.csv
│   ├── transferencias_constitucionais_nordeste.csv
│   └── bolsa_familia_urls_download.csv
├── processed/                    # Dados processados (ETL futuro)
├── logs/                         # Logs de execução
└── metadata_coleta.json          # Metadados da última execução
```

## Séries BACEN Coletadas

| Código | Descrição |
|--------|-----------|
| 25389 | IBCR Nordeste (ajuste sazonal) |
| 14084 | Saldo crédito PF - Nordeste |
| 14089 | Saldo crédito PJ - Nordeste |
| 14079 | Saldo crédito total - Nordeste |
| 4189 | SELIC acumulada mensal (anualizada) |
| 433 | IPCA mensal |
| 22109 | PIB trimestral (índice) |
| 21084 | Inadimplência PF |
| 21085 | Inadimplência PJ |
| 20539 | Crédito PF Brasil |
| 20541 | Crédito PJ Brasil |

## API Key - Portal da Transparência

Para coletar dados detalhados do Bolsa Família, é necessário uma API Key gratuita:

1. Acesse: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
2. Cadastre seu e-mail
3. Use a chave recebida conforme instruções acima

## Observações

- O SICONFI pode demorar bastante (muitas requisições com rate limit)
- Todos os dados são salvos em CSV e Parquet (quando pyarrow disponível)
- Logs detalhados são salvos em `dados_nordeste/logs/`
- O script é idempotente: rodar novamente sobrescreve os dados anteriores
- Para o ETL de uniformização, os dados brutos estarão na pasta `raw/`

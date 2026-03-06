# Bruno — Projeto Acadêmico (Tese DESP/UFC)

## Objetivo Atual

O projeto sustenta uma tese de doutorado sobre os impactos do crédito no Nordeste entre 2015 e 2025.

O foco atual não é mais apenas crescimento agregado. A variável dependente principal passou a ser emprego formal. A pergunta central é algo próximo de:

> Se houver expansão do crédito, quantos empregos formais são gerados no Nordeste?

`IBCR-NE` permanece como controle, não como variável dependente principal.

## Princípio Metodológico Importante

Neste projeto, a coleta de dados públicos não é apenas ingestão. Ela também é auditoria de qualidade da fonte.

Sempre considerar:

- cobertura temporal
- cobertura territorial
- nulos em variáveis críticas
- mudanças de schema
- duplicidade por chave
- coerência da variável ao longo do tempo

## Stack Atual

- **Frontend**: Streamlit
- **Backend**: Python modular em `pipeline/`
- **Dados**: CSV em `dados_nordeste/raw/`, `dados_nordeste/processed/` e `dados_nordeste/quality/`
- **Deploy**: Docker + Coolify

## Estrutura Real da Pipeline

```text
pipeline/
  config.py
  utils.py
  run.py
  quality.py
  extract/
    bacen.py
    siconfi.py
    transferencias.py
    bolsa_familia.py
    portal_transparencia.py
    caged_rais.py
    siof.py
    transparencia_al.py
    transparencia_pi.py
  transform/
    etl.py
    preparacao_modelo.py
```

## Fluxo Atual

1. `extract`: coleta dados brutos por fonte
2. `transform.etl`: organiza e resume saídas para análise
3. `transform.preparacao_modelo`: deflaciona e harmoniza parte das séries para bimestre
4. `quality`: audita a qualidade dos dados coletados e processados

## Entry Points Corretos

- Coleta principal: `python3 -m pipeline.run`
- Coleta modular: `python3 -m pipeline.run --modulos ...`
- ETL: `python3 -m pipeline.transform.etl`
- Preparação do modelo: `python3 -m pipeline.transform.preparacao_modelo`
- Auditoria de qualidade: `python3 -m pipeline.quality`

## Módulos de Coleta Atuais

- `bacen`: crédito, IBCR-NE, IBC-Br, SELIC, IPCA, inadimplência
- `siconfi_rreo`: resultado primário e estrutura fiscal bimestral
- `siconfi_rgf`: dívida e gestão fiscal
- `siconfi_dca`: balanço e investimento público
- `transferencias`: transferências constitucionais
- `bolsa_familia`: geração de URLs / coleta complementar
- `caged_rais`: emprego formal via FTP MTE/PDET
- `auditoria_qualidade`: verificação automatizada da qualidade dos dados

## Situação Atual das Fontes

### Fortes

- `BACEN`: eixo mais estável e completo
- `SICONFI`: cobertura ampla e boa utilidade para controles fiscais
- `CAGED`: importante para a variável dependente de emprego, com antigo + novo

### Frágeis

- `RAIS`: gaps relevantes e fragilidade em variáveis como `remuneracao_media`
- `Bolsa Família` e `BPC`: ainda não plenamente integrados como controles finais robustos
- `Transferências`: úteis, mas ainda precisam amadurecimento para a base final do modelo

## Convenções de Dados

- `raw/<fonte>/<escopo>/arquivo.csv`
- `processed/<fonte>/<uf_ou_escopo>/arquivo.csv`
- `quality/quality_report.json`
- `quality/quality_summary.csv`
- `quality/quality_report.md`

## Fatos Importantes para Próximas Sessões

- O projeto está mais maduro em coleta do que em integração analítica final.
- Ainda não existe uma base mestra única da tese com todas as variáveis integradas em um painel final.
- A harmonização bimestral está mais avançada para `BACEN` e `CAGED`.
- `RAIS` é um dos principais gargalos de robustez.
- A documentação da pipeline foi atualizada em 2026-03-06 para refletir o estado real do projeto.
- Existe agora uma camada formal de auditoria de qualidade em `pipeline/quality.py`.

## Regra de Ouro para Assistentes Futuros

Ao trabalhar neste projeto, priorizar nesta ordem:

1. clareza do objetivo atual da tese
2. qualidade e rastreabilidade dos dados públicos
3. integração analítica entre fontes
4. só depois refinamentos de dashboard ou produto tecnológico

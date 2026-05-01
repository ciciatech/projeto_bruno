# Relatório de Qualidade dos Dados

Gerado em: 2026-03-06T12:20:21.839907

## Resumo Geral

- Total de fontes auditadas: 16
- OK: 8
- Alerta: 8
- Erro: 0

## Fontes Auditadas

### bacen_raw_wide

- Status: **OK**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/bacen/nacional/bacen_sgs_wide.csv`
- Registros: 132
- Colunas: 13
- Nulos totais: 5.13%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0

### caged_antigo_raw

- Status: **ALERTA**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/caged/nordeste/caged_antigo_saldo_mensal.csv`
- Registros: 423
- Colunas: 8
- Nulos totais: 0.0%
- Cobertura temporal: 2015 a 2019
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 117
- Alertas:
  - Lacunas de continuidade: 117 combinações período-UF ausentes.

### caged_novo_raw

- Status: **OK**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/caged/nordeste/caged_saldo_mensal.csv`
- Registros: 648
- Colunas: 8
- Nulos totais: 0.0%
- Cobertura temporal: 2020 a 2025
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0

### rais_vinculos_raw

- Status: **ALERTA**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/rais/nordeste/rais_vinculos.csv`
- Registros: 47
- Colunas: 4
- Nulos totais: 25.0%
- Cobertura temporal: 2015 a 2022
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 25
- Alertas:
  - Nulos relevantes em colunas críticas: remuneracao_media
  - Anos esperados ausentes: 2019, 2020
  - Lacunas de continuidade: 25 combinações período-UF ausentes.

### siconfi_rreo_raw

- Status: **OK**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/siconfi/nordeste/siconfi_rreo_nordeste.csv`
- Registros: 1987525
- Colunas: 15
- Nulos totais: 1.57%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0

### siconfi_rgf_raw

- Status: **OK**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/siconfi/nordeste/siconfi_rgf_nordeste.csv`
- Registros: 95253
- Colunas: 15
- Nulos totais: 0.92%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0

### siconfi_dca_raw

- Status: **ALERTA**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/siconfi/nordeste/siconfi_dca_nordeste.csv`
- Registros: 232661
- Colunas: 11
- Nulos totais: 1.33%
- Cobertura temporal: 2015 a 2024
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0
- Alertas:
  - Anos esperados ausentes: 2025

### transferencias_raw

- Status: **OK**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/transferencias/nordeste/transferencias_constitucionais_nordeste.csv`
- Registros: 42205
- Colunas: 15
- Nulos totais: 1.7%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0

### bolsa_familia_uf_raw

- Status: **ALERTA**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/bolsa_familia/nordeste/bolsa_familia_uf_mensal.csv`
- Registros: 0
- Colunas: 0
- Nulos totais: None%
- Cobertura temporal: None a None
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0
- Alertas:
  - Arquivo opcional não encontrado.

### bolsa_familia_portal_raw

- Status: **ALERTA**
- Camada: `raw`
- Arquivo: `dados_nordeste/raw/bolsa_familia/nordeste/bolsa_familia_portal_transparencia.csv`
- Registros: 0
- Colunas: 0
- Nulos totais: None%
- Cobertura temporal: None a None
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0
- Alertas:
  - Arquivo opcional não encontrado.

### bacen_bimestral_processed

- Status: **OK**
- Camada: `processed`
- Arquivo: `dados_nordeste/processed/bacen/nacional/bacen_bimestral.csv`
- Registros: 66
- Colunas: 19
- Nulos totais: 1.75%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0

### caged_bimestral_processed

- Status: **OK**
- Camada: `processed`
- Arquivo: `dados_nordeste/processed/caged/nordeste/caged_bimestral.csv`
- Registros: 594
- Colunas: 8
- Nulos totais: 0.0%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0

### execucao_orcamentaria_al_processed

- Status: **ALERTA**
- Camada: `processed`
- Arquivo: `dados_nordeste/processed/execucao_orcamentaria/al/transparencia_al.csv`
- Registros: 0
- Colunas: 0
- Nulos totais: None%
- Cobertura temporal: None a None
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0
- Alertas:
  - Arquivo opcional não encontrado.

### execucao_orcamentaria_ce_processed

- Status: **ALERTA**
- Camada: `processed`
- Arquivo: `dados_nordeste/processed/execucao_orcamentaria/ce/siof_ce.csv`
- Registros: 0
- Colunas: 0
- Nulos totais: None%
- Cobertura temporal: None a None
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0
- Alertas:
  - Arquivo opcional não encontrado.

### execucao_orcamentaria_pi_processed

- Status: **ALERTA**
- Camada: `processed`
- Arquivo: `dados_nordeste/processed/execucao_orcamentaria/pi/transparencia_pi.csv`
- Registros: 0
- Colunas: 0
- Nulos totais: None%
- Cobertura temporal: None a None
- Cobertura territorial: 0 UFs
- Lacunas de continuidade: 0
- Alertas:
  - Arquivo opcional não encontrado.

### painel_tese_bimestral

- Status: **OK**
- Camada: `processed`
- Arquivo: `dados_nordeste/processed/model_ready/painel_tese_bimestral.csv`
- Registros: 594
- Colunas: 37
- Nulos totais: 9.69%
- Cobertura temporal: 2015 a 2025
- Cobertura territorial: 9 UFs
- Lacunas de continuidade: 0

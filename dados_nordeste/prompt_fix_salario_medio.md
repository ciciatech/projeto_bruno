# Investigação e Correção: Campo `salario_medio` nos CSVs do CAGED

## Contexto

Durante a consolidação dos dados do CAGED para o Nordeste, foi identificado que o campo `salario_medio` está **incorreto** em vários CSVs da pipeline de extração. O problema afeta tanto dados brutos (`raw/`) quanto processados (`processed/`).

## Problema Identificado

### Arquivos de saldo mensal — `salario_medio` é cópia exata de `total_movimentacoes`

Nos CSVs de saldo mensal (antigo e novo), a coluna `salario_medio` contém **exatamente** o mesmo valor que `total_movimentacoes` em **100% das linhas**.

**Evidência — `raw/caged/nordeste/caged_antigo_saldo_mensal.csv`:**
```
ano   mes  sigla_uf  salario_medio  total_movimentacoes
2015    2        AL          18203                18203
2015    2        BA         119046               119046
```

**Evidência — `raw/caged/nordeste/caged_saldo_mensal.csv`:**
```
ano   mes  sigla_uf  salario_medio  total_movimentacoes
2020    1        AL          20295                20295
2020    1        BA          94242                94242
```

Valores na faixa de 8.000–180.000 são incoerentes para salário médio mensal em R$. O padrão esperado seria algo entre R$ 1.000 e R$ 5.000.

### Arquivos de perfil e setor — `salario_medio` ≈ `total_movimentacoes / 12`

Nos CSVs de perfil e por setor (CAGED Novo), `salario_medio` é aproximadamente `total_movimentacoes / 12` em ~94% das linhas, sugerindo que o campo recebeu a **média mensal de movimentações**, não o salário médio real.

**Evidência — `raw/caged/nordeste/caged_por_perfil.csv`:**
```
ano  sigla_uf  sexo  salario_medio  total_movimentacoes
2020       AL     1       660.5000                 7926   (7926/12 = 660.5)
2020       AL     1      2026.0833                24313   (24313/12 = 2026.08)
```

### Arquivos afetados

| Arquivo | Tipo de erro | Cobertura |
|---------|-------------|-----------|
| `raw/caged/nordeste/caged_antigo_saldo_mensal.csv` | salario_medio == total_movimentacoes | 100% |
| `raw/caged/nordeste/caged_saldo_mensal.csv` | salario_medio == total_movimentacoes | 100% |
| `processed/caged/nordeste/caged_antigo_saldo_mensal.csv` | salario_medio == total_movimentacoes | 100% |
| `processed/caged/nordeste/caged_saldo_mensal.csv` | salario_medio == total_movimentacoes | 100% |
| `processed/caged/<uf>/caged_saldo_mensal.csv` (9 UFs) | salario_medio == total_movimentacoes | 100% |
| `processed/caged/<uf>/caged_antigo_saldo_mensal.csv` (9 UFs) | salario_medio == total_movimentacoes | 100% |
| `processed/caged/nordeste/caged_bimestral.csv` | salario_medio == total_movimentacoes | 100% |
| `raw/caged/nordeste/caged_por_perfil.csv` | salario_medio ≈ total_mov/12 | ~94% |
| `raw/caged/nordeste/caged_por_setor.csv` | salario_medio ≈ total_mov/12 | ~92% |
| `raw/caged/nordeste/caged_antigo_por_perfil.csv` | padrão similar (divisor variável) | ~29% |

## Ações Solicitadas

1. **Investigar a origem do erro na pipeline de extração.** Localizar o script ou etapa que popula o campo `salario_medio` e verificar se:
   - Há um bug de atribuição (coluna errada sendo copiada)
   - A API/fonte de dados do CAGED realmente fornece salário médio ou se o campo foi inferido incorretamente
   - O campo `salario_medio` do CAGED Antigo (PDET/MTE) e do Novo CAGED (eSocial) têm fontes distintas

2. **Corrigir a extração.** Se a fonte de dados (API do CAGED ou microdados) fornece o salário médio real, corrigir o mapeamento no script de extração para que `salario_medio` receba o valor correto.

3. **Se a fonte não fornecer salário médio**, considerar:
   - Remover o campo `salario_medio` dos CSVs para evitar confusão
   - Ou renomear para o que ele realmente representa (ex: `media_mensal_movimentacoes`)

4. **Reprocessar os CSVs afetados** após a correção, incluindo os arquivos `processed/` derivados e o `caged_bimestral.csv`.

5. **Validação pós-correção**: verificar que os novos valores de `salario_medio` estejam na faixa esperada (R$ 800–R$ 5.000 para o Nordeste) e que não haja correlação espúria com `total_movimentacoes`.

## Notas

- A planilha `caged_consolidado.xlsx` foi gerada **sem** o campo `salario_medio` para não propagar dados incorretos em entregas executivas.
- Os demais campos (admissões, desligamentos, saldo, total_movimentacoes) foram verificados e estão consistentes entre as fontes.

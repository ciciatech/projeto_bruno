# Providências — Pipeline Regional CE (Tese Bruno Cardoso)

**Data:** 2026-04-29
**Contexto:** Reformulação solicitada pelo Prof. Paulo (mensagens em `mudanca.md` + áudio adicional sobre investimento). Pipeline regional CE construída em paralelo ao pipeline NE legado.

---

## 1. O que está pronto e validado

### 1.1 Componentes implementados

| Componente | Arquivo | Validação |
|------------|---------|-----------|
| Mapeamento município → região CE (184 × 14) | `pipeline/regioes_ce.py` + `pipeline/data/municipios_ce_regioes.csv` | ✓ 184 municípios, 14 regiões, casamento 100% com IBGE |
| Tabela PIB shares CE/BR e CE/NE | `pipeline/pib_shares.py` + `pipeline/data/pib_shares_ce.csv` | ✓ 11 anos cobertos (2015-2025), shares entre 2.0-2.2% e 14.4-15.0% |
| Coletor BACEN (com IBCR-CE) | `pipeline/extract/bacen.py` | ✓ Série SGS 25380 testada (cobertura 100%) |
| Coletor ESTBAN BNB municipal | `pipeline/extract/estban.py` | ✓ Parser testado com mock — filtra CE+BNB corretamente |
| Coletor Bolsa Família CE municipal | `pipeline/extract/bolsa_familia.py` | ✓ Testado real jan/2024: 184/184 municípios, R$ 949M |
| Coletor BPC municipal | `pipeline/extract/bpc.py` | Estrutura OK (mesma lógica do BF, não testado real) |
| Coletor transferências constitucionais STN | `pipeline/extract/transferencias_municipais.py` | ✓ Testado real fev/2024: R$ 712M FPM + R$ 680M FUNDEB |
| Coletor invest_federal RREO (3 componentes) | `pipeline/extract/invest_federal.py` | ✓ Testado real jan/2024: R$ 7M direto + R$ 2.6M nacional×2.2% = R$ 10M |
| Coletor IpeaData FBCF mensal | `pipeline/extract/ipea_fbcf.py` | ✓ 361 obs, 132/132 meses 2015-2025, R$ 788bi FBCF Brasil 2024 |
| Coletor CAGED municipal | `pipeline/extract/caged_municipal.py` | Estrutura OK, agregação validada com mock (não rodado real — pesado, FTP MTE) |
| Adapter SEFAZ-CE | `pipeline/extract/sefaz_ce.py` | Estrutura OK — aguarda arquivos manuais |
| Adapter planilha Bruno (invest. municipal) | `pipeline/extract/invest_municipal_planilha.py` | Estrutura OK — aguarda planilha do Bruno |
| Construção do painel regional | `pipeline/transform/preparacao_modelo_regional.py` | ✓ 1848 linhas (14×132), inclui invest_total_ce e ibcr_ce com cobertura 100% |
| CLI orquestrador | `pipeline/run.py` | ✓ Flags `--apenas-ce`, `--full-ce`, `--painel-ce`, `--modulos-ce` |
| Documentação | `docs/README.md` (seção CE), `docs/dicionario_dados.md` (seção 6) | ✓ Atualizadas |

### 1.2 Painel resultante (atual)

`dados_nordeste/processed/model_ready/painel_regional_ce_mensal.csv`

- **1848 linhas** (14 regiões × 132 meses 2015-01 a 2025-12)
- **11 colunas** preenchidas hoje:
  - Chave: `regiao_codigo`, `regiao_nome`, `ano`, `mes`
  - SIOF (anual replicado em meses): `siof_anual_empenhado`, `siof_anual_pago`, `siof_anual_dotacao`, `siof_anual_n_acoes` — **cobertura 0%**, ver §3.1
  - Macro estadual: `ibcr_ce` (cobertura 100%)
  - Investimento total estimado: `invest_total_ce_r_2010_mi`, `share_pib_ce_br` (cobertura 100%)

Ao rodar os coletores pesados (BF, BPC, transf, invest_fed, CAGED), o painel cresce para ~30-40 colunas.

---

## 2. O que ainda falta implementar

### 2.1 Tarefa não iniciada

**Coletor `invest_municipal` SICONFI bimestral com filtros anti-erro** — substituir o adapter atual (que lê planilha do Bruno) por coletor automatizado da API SICONFI/STN, **com heurística** para detectar quando uma prefeitura reporta valor corrente em vez de acumulado bimestral. Conforme áudio do Prof. Paulo: "às vezes as prefeituras divulgam os dados de maneira equivocada — em vez do dado acumulado, colocam o corrente. A gente tem que criar uns truquezinhos, uns filtrozinhos para perceber quando eles erram."

**Heurística sugerida**: comparar bimestre N+1 com bimestre N. Se valor N+1 < valor N, suspeitar de relato corrente — usar diferença com bimestres adjacentes ou descartar e marcar.

**Esforço estimado**: 4-6h. Reaproveita parte do coletor SICONFI legado (`pipeline/extract/siconfi.py`, hoje pega RREO estadual NE).

### 2.2 Cálculo do `invest_privado` residual

Conforme metodologia do Prof. Paulo:
```
invest_privado_CE = invest_total_CE - invest_estadual - invest_municipal - invest_federal
```

**Bloqueador**: harmonização monetária. As fontes estão em bases diferentes:
- `invest_total_ce_r_2010_mi` (FBCF) → R$ a preços de **2010**
- SIOF (estadual), SICONFI (municipal), Portal Transp (federal) → R$ **correntes**

**Decisão necessária**: qual ano-base usar?
- Opção A — R$ 2010 (base do FBCF): aplicar IPCA acumulado para deflacionar SIOF/SICONFI/Portal Transp
- Opção B — R$ 2024 ou 2025 (mais "atual"): inflar FBCF e deflacionar os demais

A pipeline já coleta IPCA mensal via BACEN-SGS (série 433). Implementação: ~2-3h.

### 2.3 Distribuição entre regiões

`invest_total_CE` e `invest_federal_CE` são valores **estaduais agregados** — replicados nas 14 regiões. Para análises regionais, o pesquisador precisa decidir como distribuir:
- Por share PIB regional (Tabela 2 IPECE TD 111/2015: Grande Fortaleza 67%, Cariri 7%, etc.)
- Ou por outra ponderação (população, FBCF setorial, etc.)

Não é urgente — pode ser feito direto na fase econométrica.

---

## 3. Dependências externas (ações fora do código)

### 3.1 SIOF histórico CE (2015-2025)

**Status**: o arquivo `dados_nordeste/processed/execucao_orcamentaria/ce/siof_obras_regiao.csv` só contém **2026** (15 linhas). O coletor SIOF (`pipeline/extract/siof.py`) está pronto e estável, mas precisa rodar para o histórico.

**Ação**: rodar
```bash
python3 -m pipeline.run --modulos siof_ce  # ou via ETL atual
```
SIOF é WebForms ASP.NET com ViewState — **demorado** (algumas horas para 11 anos). Pode rodar em background.

### 3.2 ESTBAN BNB

**Status**: coletor é adapter; aguarda arquivos. Origem confirmada com Prof. Paulo: BCB removeu URL pública estável (IN BCB 502/2024); fontes possíveis:
- Sistema Cosif do BCB (UI manual)
- **Recomendado**: Base dos Dados via BigQuery — exportar `basedosdados.br_bcb_estban.municipio` filtrando `id_uf = '23'`. Tier free (1TB/mês) é suficiente.

**Ação**: alguém da equipe acessa Base dos Dados, exporta CSVs mensais (2015-01 a 2025-12) e coloca em `dados_nordeste/raw/estban/`. Coletor processa automaticamente.

### 3.3 Planilha invest. municipal do Bruno

**Status**: pendente. Coletor pronto (adapter `invest_municipal_planilha.py`).

**Ação**: Bruno entrega o XLSX/CSV; salvar em `dados_nordeste/raw/invest_municipal/`. Schema esperado: colunas para município (nome ou cod_ibge), ano, mês (ou data), valor.

**Alternativa preferida pelo Prof. Paulo**: substituir esse adapter pelo coletor SICONFI automático (§2.1).

### 3.4 SEFAZ-CE (transferências estaduais cota-parte)

**Status**: adapter pronto; auto-download bloqueado por WAF do Ceará Transparente.

**Ação**: download manual do portal Ceará Transparente → `dados_nordeste/raw/sefaz_ce/`. **Importância**: secundária (já temos transferências federais STN cobrindo FPM/FUNDEB; SEFAZ-CE adicionaria ICMS/IPVA cota-parte).

### 3.5 Chave API Portal da Transparência

**Status**: opcional para os coletores BF/BPC/invest_federal (eles usam **CSV bulk**, não a API com chave). A chave só seria necessária se quiséssemos coleta incremental por município via API REST — fora do escopo atual.

---

## 4. Roteiro recomendado de execução

### Para o Bruno/equipe (em ordem)

**Etapa 1 — preparar entradas externas** (1-2 dias):
1. Bruno entrega planilha de invest. municipal → `dados_nordeste/raw/invest_municipal/` *(ou aguarda coletor SICONFI da §2.1)*
2. Equipe baixa ESTBAN da Base dos Dados → `dados_nordeste/raw/estban/`
3. (Opcional) Equipe baixa SEFAZ-CE manualmente → `dados_nordeste/raw/sefaz_ce/`

**Etapa 2 — rodar coletas leves** (~2 horas):
```bash
python3 -m pipeline.run --modulos-ce bacen estban transferencias_municipais invest_municipal sefaz_ce
python3 -m pipeline.extract.ipea_fbcf
```

**Etapa 3 — rodar coletas pesadas** (~6-8 horas — pode rodar overnight):
```bash
python3 -m pipeline.run --modulos-ce bolsa_familia_ce bpc invest_federal
```
Cada um baixa ZIPs de ~5-100 MB por mês × 132 meses. Mantém cache em `dados_nordeste/raw/{programa}/` e `_meses_consolidados/`.

**Etapa 4 — coletar CAGED municipal** (~12-24 horas — opcional para emprego/salário regional):
```bash
python3 -m pipeline.run --modulos-ce caged_municipal
```
Reaproveita ~500 GB de FTP do MTE/PDET — só rode se for usar emprego regional, senão SIOF + transf cobre o controle.

**Etapa 5 — montar painel final**:
```bash
python3 -m pipeline.run --painel-ce
```

**Etapa 6 — cálculo do residual privado** (após eu implementar §2.2):
```bash
python3 -m pipeline.run --painel-ce --com-residual-privado --ano-base 2010
```
*(flag a criar)*

### Para mim (próxima sessão)

1. **#16** — implementar coletor SICONFI municipal com filtros anti-erro
2. **#2.2** — adicionar deflacionamento e cálculo do residual privado no painel
3. (Opcional) Confirmar com Prof. Paulo: ano-base do residual? Já podemos rodar coletas pesadas?

---

## 5. Decisões pendentes do Prof. Paulo

Listadas em ordem de urgência:

1. **Ano-base do investimento privado residual**: R$ 2010 (FBCF nativo) ou R$ 2024/2025 (mais atual, exige inflar FBCF)?
2. **CAGED municipal vale o custo (~24h FTP)?** Ou usar emprego total estadual e perder a granularidade regional?
3. **SEFAZ-CE manual é prioridade?** Adiciona ICMS/IPVA cota-parte sobre o que já temos do STN.
4. **Investimento municipal: planilha do Bruno (rápido) ou coletor SICONFI automático (mais robusto, +6h de implementação)?**

---

## 6. Riscos identificados

| Risco | Mitigação |
|-------|-----------|
| Heurística anti-erro do invest_municipal pode falhar para edge cases | Logar todos os meses suspeitos em arquivo de auditoria; revisão manual no painel |
| Shares PIB CE/BR e CE/NE para 2024-2025 são estimados | Carry-forward documentado em `pib_shares_ce.csv`; substituir quando IBGE publicar |
| Forecast FBCF Brasil 2025 (IpeaData já tem jan/2026) | Atualmente IpeaData publica até jan/2026; falta apenas fev-dez/2025? Verificar — pode usar média móvel ou aguardar IpeaData |
| ESTBAN sem auto-download | Documentado claramente em adapter; processo manual é ~30 min |
| FBCF e SIOF em ano-base diferentes | §2.2 resolve via IPCA |

---

## 7. Auto-sync com Mac Mini (deploy do código + execução das coletas)

Para evitar rodar coletas pesadas no laptop, configuramos auto-sync git com o Mac Mini reproduzindo o padrão usado no BichoPix.

### Arquivos instalados neste repo

| Arquivo | Função |
|---------|--------|
| `scripts/git-auto-pull.sh` | Roda no Mac Mini via LaunchAgent a cada 5 min — `git pull --ff-only origin dev` se houver novidade |
| `.githooks/post-commit` | No laptop: faz `git push origin dev` após cada commit |
| `.githooks/pre-push` | Antes do push, valida que `pipeline/` compila e importa sem erro |
| `scripts/setup-macmini-sync.sh` | Roda 1x no Mac Mini — clona repo, configura hooks, cria venv, instala LaunchAgent |
| `scripts/run_coletas.sh` | Disparador das ondas de coleta (`onda1`, `onda2`, `onda3`, `painel`, `full`) |

### Setup inicial no Mac Mini (1ª vez)

```bash
# No Mac Mini (via SSH ou direto):
mkdir -p ~/dev/academico
cd ~/dev/academico
git clone -b dev https://github.com/ciciatech/projeto_bruno.git bruno
cd bruno
bash scripts/setup-macmini-sync.sh
```

Resultado: LaunchAgent `com.bruno-pipeline.autopull` ativo, puxando código a cada 5 min.

### Disparar coletas no Mac Mini (uso recorrente)

Do laptop, via SSH:

```bash
# Onda 1 (leve, ~5 min)
ssh mac-mini "bash ~/dev/academico/bruno/scripts/run_coletas.sh onda1"

# Onda 2 (pesada, ~6-10h — rode com nohup ou tmux para sobreviver à sessão SSH)
ssh mac-mini "nohup bash ~/dev/academico/bruno/scripts/run_coletas.sh onda2 > /tmp/onda2.log 2>&1 &"

# Acompanhar
ssh mac-mini "tail -f ~/.local/log/bruno-pipeline-coletas.log"
```

### Fluxo de trabalho típico

1. Faço commit no laptop → post-commit auto-pusha pra `origin/dev`
2. Mac Mini puxa em até 5 min via LaunchAgent
3. Disparo coleta no Mac Mini via SSH
4. Resultados ficam em `dados_nordeste/processed/...` no Mac Mini
5. Quando coleta termina, **outro commit no Mac Mini** (manual ou via cron) sobe os processed para `origin/dev`
6. Laptop puxa de volta com `git pull`

**Observação**: para o item 5 ficar automático, ainda falta criar um cron/launchd que faça `git add dados_nordeste/processed/ && git commit -m "data: coleta XYZ" && git push` após cada onda. Posso fazer numa próxima iteração se for útil.

---

## 8. Resumo de comandos úteis

```bash
# Validar painel atual sem refazer nada
python3 -m pipeline.run --painel-ce

# Rodar tudo (NÃO recomendado de cara — etapas separadas é melhor para diagnosticar erros)
python3 -m pipeline.run --full-ce

# Rodar só BACEN (rápido, traz IBCR-CE)
python3 -m pipeline.run --apenas-bacen

# Rodar só FBCF mensal (rápido — API IpeaData)
python3 -m pipeline.extract.ipea_fbcf

# Sanity check do mapeamento município → região
python3 -m pipeline.regioes_ce
```

# Catálogo Completo de Indicadores de Infraestrutura — IPECEDATA

O sistema está disponível em **http://ipecedata.ipece.ce.gov.br/ipece-data-web/** e organiza as informações em dimensões temáticas, com dados desagregados por **Ceará, Região de Planejamento, Região Metropolitana e Município**, além de série histórica por ano. Abaixo o mapeamento completo das 6 dimensões de infraestrutura identificadas:

---

## 1. ENERGIA

**Subtema: Infraestrutura energética** — Abrangência: Ceará (nível estadual)

Os indicadores existem na forma Acumulado e Realizado (no ano):
- Ampliação da capacidade instalada das subestações (MVA) — série 2001–2018
- Construção de linhas de transmissão (km)
- Construção de subestações (unidades)
- Construção/Reforma de rede de distribuição (km)

**Subtema: Consumo de energia elétrica** — Abrangência: até Município

- Consumo de energia elétrica Total (MWh)
- Consumo de energia elétrica - Comercial
- Consumo de energia elétrica - Industrial
- Consumo de energia elétrica - Próprio (autogeração/serviços públicos)
- (e demais classes: Residencial, Rural, Iluminação pública etc. — lista parcialmente visível)

**Subtema: Consumidores de energia elétrica** — Abrangência: até Município

- Consumidores de energia elétrica Total
- Consumidores - Comercial
- Consumidores - Industrial
- Consumidores - Próprio
- (idem demais classes)

---

## 2. RECURSOS HÍDRICOS

**Subtema: Infraestrutura hídrica** — Abrangência: até Município

- Capacidade das barragens construídas — por barragem específica:
  - Barragem Angicos (Riacho Juazeiro)
  - Barragem Aracoiaba (Riacho Aracoiaba)
  - Barragem Arneiroz II (Riacho Jaguaribe)
  - Barragem Barra Velha (Riacho Santo Antônio) — e outras dezenas de barragens/açudes

> Este conjunto de dados permite mapear a capacidade hídrica instalada em cada município do Ceará.

---

## 3. HABITAÇÃO

**Subtema: Domicílios** — Abrangência: até Município

- Distribuição de domicílios segundo número de moradores (série 2016–2024)
- Moradores em domicílios segundo existência de bens duráveis (2024):
  - Geladeira (9.032 mil unid.), Máquina de lavar (4.230), Motocicleta (3.749), Carro (2.696), Carro+Motocicleta (1.120)

---

## 4. SANEAMENTO

**Subtema: Abastecimento de água** — Abrangência: até Município, por Censo (1991, 2000, 2010, 2022)

- Domicílios com abastecimento de água (por rede geral / poço / cisterna etc.) — por município
- Séries históricas desde 1991 para todos os 184 municípios

**Subtema: Esgotamento sanitário** — Abrangência: até Município

- Domicílios com esgotamento sanitário (rede coletora, fossa, céu aberto etc.)

**Subtema: Destino do lixo** — Abrangência: até Município

- Domicílios segundo destino do lixo (coleta, queima, enterro etc.)

---

## 5. TRANSPORTES

**Subtema: Transporte aéreo** — Abrangência: Ceará (dados do Aeroporto de Fortaleza)

Indicadores por categoria e tipo de voo (regular/charter), desagregados por: embarque/desembarque/trânsito:
- Movimento de aeronaves
- Passageiros
- Carga aérea
- Correios (encomendas)

**Subtema: Transporte ferroviário** — Abrangência: Ceará

- Indicadores de movimentação ferroviária

**Subtema: Transporte marítimo** — Abrangência: Ceará (Porto de Fortaleza e Porto do Pecém)

- Movimentação de cargas e embarcações nos portos

**Subtema: Transporte rodoviário** — Abrangência: até Município

- Carteiras de habilitação expedidas (Total, Homens, Mulheres)
- Frota de veículos por tipo de combustível:
  - Camioneta, Diesel, Gás natural (e outros tipos — Álcool, Gasolina, Elétrico etc.)

---

## 6. COMUNICAÇÕES

**Subtema: Correios e telégrafos** — Abrangência: Ceará

- Tráfego postal Total — série histórica 2000–2024 (pico ~250 milhões em 2008; queda para ~50M em 2023)

**Subtema: Radiodifusão de som** — Abrangência: Ceará/Município

- Número de emissoras de rádio

**Subtema: Radiodifusão de som e imagem** — Abrangência: Ceará/Município

- Número de emissoras de TV

**Subtema: Telefonia** — Abrangência: Ceará

- Telefonia móvel - Acessos (série 2005–2024: subiu de ~4M para pico de ~12M em 2014, depois estabilizou ~9-10M)
- Telefonia fixa - Acessos

---

## COMO USAR O IPECEDATA NA PRÁTICA

Para extrair os dados por município para cada indicador, o fluxo no sistema é:

1. Selecionar a dimensão e subtema no menu lateral
2. Na aba **"Apresentação"**: escolher o indicador desejado no dropdown
3. Definir a **Abrangência Geográfica**: Ceará / Região de Planejamento / Região Metropolitana / Município
4. Filtrar por **Ano** (ou "Tudo" para a série completa)
5. Clicar na aba **"Indicadores"** para ver a tabela de dados por município
6. Usar **"Exibir no Tableau Public"** para acessar a visualização interativa completa com exportação de dados

---

## PRÓXIMOS PASSOS SUGERIDOS

Com esse catálogo mapeado, podemos agora:

**A.** Extrair os dados de um indicador específico (ex: frota de veículos por município, ou domicílios com abastecimento de água) navegando pelo sistema e gerando a tabela completa.

**B.** Cruzar com os dados do SNIS (saneamento) e IBGE/Censo 2022 para complementar os gaps do Ipecedata.

**C.** Montar uma planilha consolidada com todos os indicadores de infraestrutura por município, usando o Ipecedata como espinha dorsal.

Qual dimensão ou indicador você quer aprofundar primeiro?
/* Dados sintéticos plausíveis — Brasil 2020-2025
   Padrões reais: SP/RJ/MG concentração SE; matopiba (BA/MA/PI/TO) crescimento agro;
   AM/PA mineração; SC/PR indústria; NE corredor de energia renovável.
   ----------------------------------------------------------------------------- */

const REGIONS = {
  N:  { name: 'Norte',         color: 'var(--cat-3)' },
  NE: { name: 'Nordeste',      color: 'var(--cat-1)' },
  CO: { name: 'Centro-Oeste',  color: 'var(--cat-4)' },
  SE: { name: 'Sudeste',       color: 'var(--cat-2)' },
  S:  { name: 'Sul',           color: 'var(--cat-5)' },
};

const SECTORS = [
  { key: 'industria',   name: 'Indústria',     color: 'var(--cat-1)' },
  { key: 'servicos',    name: 'Serviços',      color: 'var(--cat-2)' },
  { key: 'agro',        name: 'Agropecuária',  color: 'var(--cat-3)' },
  { key: 'construcao',  name: 'Construção',    color: 'var(--cat-4)' },
  { key: 'comercio',    name: 'Comércio',      color: 'var(--cat-5)' },
  { key: 'admpublica',  name: 'Adm. Pública',  color: 'var(--cat-6)' },
];

// Investimento privado anunciado (2024) por UF — R$ milhões
const INVESTMENT_BY_UF = {
  SP: 142800, RJ: 89300, MG: 68400, RS: 41200, PR: 38900, SC: 32100, BA: 28700,
  GO: 24300, PE: 21800, CE: 19400, ES: 17200, MT: 16800, MS: 14900, PA: 13700,
  AM: 11200, MA: 9800,  PB: 7600,  RN: 8400,  AL: 5300,  SE: 4900,  PI: 6100,
  DF: 12400, TO: 4100,  RO: 3800,  AC: 1900,  AP: 1700,  RR: 1400,
};

// Variação YoY emprego formal 2024 vs 2023 (%)
const EMPLOYMENT_GROWTH_YOY = {
  SP: 2.4, RJ: 1.8, MG: 3.1, RS: 1.2, PR: 3.4, SC: 4.1, BA: 4.8,
  GO: 5.2, PE: 3.9, CE: 4.2, ES: 2.7, MT: 6.4, MS: 5.1, PA: 4.7,
  AM: 1.4, MA: 7.2, PB: 3.1, RN: 2.8, AL: 2.4, SE: 1.9, PI: 6.8,
  DF: 0.9, TO: 5.4, RO: 3.2, AC: 0.4, AP: -0.8, RR: -1.2,
};

// Taxa de desemprego (3T/2024) por UF (%)
const UNEMPLOYMENT = {
  SP: 6.4, RJ: 8.1, MG: 5.8, RS: 5.2, PR: 4.6, SC: 3.4, BA: 11.2,
  GO: 5.4, PE: 10.8, CE: 9.4, ES: 6.7, MT: 4.1, MS: 4.4, PA: 8.6,
  AM: 9.2, MA: 8.8, PB: 8.4, RN: 9.6, AL: 9.1, SE: 9.4, PI: 8.2,
  DF: 8.7, TO: 6.4, RO: 5.1, AC: 9.8, AP: 12.4, RR: 7.6,
};

// Série temporal 2020-2025 trimestral — investimento por região (R$ bi)
const QUARTERS = ['1T20','2T20','3T20','4T20','1T21','2T21','3T21','4T21','1T22','2T22','3T22','4T22','1T23','2T23','3T23','4T23','1T24','2T24','3T24','4T24','1T25','2T25'];

const INVESTMENT_TIMESERIES = {
  SE: [78,52,68,82,84,91,98,108,112,118,124,131,128,134,141,148,152,158,164,172,168,178],
  S:  [42,29,38,46,48,52,56,61,63,66,69,72,71,74,77,80,82,84,87,90,89,93],
  NE: [28,22,26,32,34,37,40,44,46,49,52,55,57,60,64,68,72,76,80,84,86,90],
  CO: [22,18,21,25,27,30,33,36,38,40,42,44,46,49,52,55,58,61,64,67,69,72],
  N:  [14,11,13,16,17,19,21,23,24,25,26,28,29,30,32,34,35,37,38,40,41,43],
};

const EMPLOYMENT_TIMESERIES = {
  SE: [11.2,9.8,10.1,10.4,10.6,10.9,11.2,11.4,11.5,11.6,11.7,11.8,11.9,12.0,12.1,12.2,12.3,12.4,12.5,12.6,12.6,12.7],
  S:  [4.8,4.4,4.5,4.6,4.7,4.8,4.9,5.0,5.1,5.1,5.2,5.2,5.3,5.3,5.4,5.4,5.5,5.5,5.6,5.6,5.7,5.7],
  NE: [5.2,4.7,4.8,4.9,5.0,5.1,5.2,5.3,5.4,5.5,5.6,5.7,5.8,5.9,6.0,6.1,6.2,6.3,6.4,6.5,6.6,6.7],
  CO: [2.4,2.2,2.3,2.4,2.5,2.5,2.6,2.7,2.7,2.8,2.8,2.9,2.9,3.0,3.0,3.1,3.1,3.2,3.2,3.3,3.4,3.4],
  N:  [1.6,1.5,1.5,1.6,1.6,1.7,1.7,1.7,1.8,1.8,1.8,1.9,1.9,1.9,2.0,2.0,2.0,2.1,2.1,2.1,2.2,2.2],
};

// Composição setorial por região (% do PIB regional, 2024)
const SECTOR_BY_REGION = [
  { x: 'Norte',     industria: 24, servicos: 41, agro: 12, construcao: 7, comercio: 11, admpublica: 5 },
  { x: 'Nordeste',  industria: 19, servicos: 48, agro: 9,  construcao: 6, comercio: 13, admpublica: 5 },
  { x: 'C-Oeste',   industria: 14, servicos: 38, agro: 28, construcao: 5, comercio: 11, admpublica: 4 },
  { x: 'Sudeste',   industria: 28, servicos: 51, agro: 4,  construcao: 5, comercio: 10, admpublica: 2 },
  { x: 'Sul',       industria: 31, servicos: 44, agro: 11, construcao: 4, comercio: 9,  admpublica: 1 },
];

// UF metadados para scatter
const UF_META = {
  SP: { region: 'SE', pop: 46.6 }, RJ: { region: 'SE', pop: 17.5 }, MG: { region: 'SE', pop: 21.4 }, ES: { region: 'SE', pop: 4.1 },
  RS: { region: 'S',  pop: 11.4 }, PR: { region: 'S',  pop: 11.6 }, SC: { region: 'S',  pop: 7.6 },
  BA: { region: 'NE', pop: 14.9 }, PE: { region: 'NE', pop: 9.7 },  CE: { region: 'NE', pop: 9.2 }, MA: { region: 'NE', pop: 7.1 },
  PB: { region: 'NE', pop: 4.0 },  RN: { region: 'NE', pop: 3.4 },  AL: { region: 'NE', pop: 3.4 }, SE: { region: 'NE', pop: 2.3 }, PI: { region: 'NE', pop: 3.3 },
  GO: { region: 'CO', pop: 7.2 },  MT: { region: 'CO', pop: 3.7 },  MS: { region: 'CO', pop: 2.9 }, DF: { region: 'CO', pop: 3.1 },
  PA: { region: 'N',  pop: 8.7 },  AM: { region: 'N',  pop: 4.3 },  TO: { region: 'N',  pop: 1.6 }, RO: { region: 'N',  pop: 1.8 },
  AC: { region: 'N',  pop: 0.9 },  AP: { region: 'N',  pop: 0.9 },  RR: { region: 'N',  pop: 0.7 },
};

// Top municípios por investimento (2024)
const TOP_MUNI = [
  { m: 'São Paulo',           uf: 'SP', v: 38400 },
  { m: 'Rio de Janeiro',      uf: 'RJ', v: 22100 },
  { m: 'Belo Horizonte',      uf: 'MG', v: 14800 },
  { m: 'Curitiba',            uf: 'PR', v: 11200 },
  { m: 'Camaçari',            uf: 'BA', v: 10400 },
  { m: 'Macaé',               uf: 'RJ', v:  9800 },
  { m: 'Campinas',            uf: 'SP', v:  9600 },
  { m: 'Joinville',           uf: 'SC', v:  8400 },
  { m: 'Suape (Ipojuca)',     uf: 'PE', v:  7900 },
  { m: 'Anápolis',            uf: 'GO', v:  6400 },
];

// Pipeline de coletores
const PIPELINE = [
  { fonte: 'CAGED — MTE',                    cobertura: 100, lastUpdate: '2026-04-28 03:14', cadencia: 'Mensal',     status: 'ok',    delta: 'Estável',     records: '1.2 M' },
  { fonte: 'RAIS — MTE',                     cobertura: 100, lastUpdate: '2026-03-15 22:00', cadencia: 'Anual',      status: 'ok',    delta: '—',           records: '54.8 M' },
  { fonte: 'PNAD-C — IBGE',                  cobertura: 100, lastUpdate: '2026-04-25 09:30', cadencia: 'Trimestral', status: 'ok',    delta: 'Estável',     records: '847 k' },
  { fonte: 'BNDES — Operações',              cobertura:  98, lastUpdate: '2026-04-22 14:11', cadencia: 'Quinzenal',  status: 'ok',    delta: '+2.1k novos', records: '128 k' },
  { fonte: 'Investe SP — Anúncios',          cobertura:  87, lastUpdate: '2026-04-27 18:42', cadencia: 'Diária',     status: 'warn',  delta: '24h sem dados',records: '4.2 k' },
  { fonte: 'IBGE — PIM-PF (Industrial)',     cobertura: 100, lastUpdate: '2026-04-12 10:00', cadencia: 'Mensal',     status: 'ok',    delta: 'Estável',     records: '184 k' },
  { fonte: 'CNAE — Receita Federal',         cobertura: 100, lastUpdate: '2026-04-26 02:00', cadencia: 'Mensal',     status: 'ok',    delta: 'Estável',     records: '21.3 M' },
  { fonte: 'Conab — Safras',                 cobertura:  92, lastUpdate: '2026-04-20 11:15', cadencia: 'Mensal',     status: 'ok',    delta: '+ revisão',   records: '12 k' },
  { fonte: 'ANEEL — Geração',                cobertura:  76, lastUpdate: '2026-04-18 09:00', cadencia: 'Mensal',     status: 'warn',  delta: 'Schema novo',records: '3.4 k' },
  { fonte: 'Comex — Exportações UF',         cobertura: 100, lastUpdate: '2026-04-15 06:30', cadencia: 'Mensal',     status: 'ok',    delta: 'Estável',     records: '892 k' },
  { fonte: 'SUDAM/SUDENE — Incentivos',      cobertura:  64, lastUpdate: '2026-03-30 16:42', cadencia: 'Trimestral', status: 'bad',   delta: 'Parser falhando',records: '8.1 k' },
  { fonte: 'IPCA-Regional — IBGE',           cobertura: 100, lastUpdate: '2026-04-10 09:00', cadencia: 'Mensal',     status: 'ok',    delta: 'Estável',     records: '64 k' },
];

const ALERTS = [
  { sev: 'bad',  src: 'SUDAM/SUDENE',   msg: 'Parser HTML quebrou em 2026-03-30. 4 ciclos pendentes.', age: '29d' },
  { sev: 'warn', src: 'ANEEL',          msg: 'Mudança de schema na coluna `subsistema`. Mapeamento manual ativo.', age: '11d' },
  { sev: 'warn', src: 'Investe SP',     msg: 'Sem novos registros há 24h. Esperado: ~80/dia.', age: '24h' },
  { sev: 'info', src: 'IBGE — PIM-PF',  msg: 'Revisão metodológica anunciada para 2T/2026.', age: '3d' },
];

Object.assign(window, {
  REGIONS, SECTORS,
  INVESTMENT_BY_UF, EMPLOYMENT_GROWTH_YOY, UNEMPLOYMENT,
  QUARTERS, INVESTMENT_TIMESERIES, EMPLOYMENT_TIMESERIES,
  SECTOR_BY_REGION, UF_META,
  TOP_MUNI, PIPELINE, ALERTS,
});

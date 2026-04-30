/**
 * 14 regiões de planejamento SEPLAG/IPECE com layout simbólico para tile-grid.
 *
 * As coordenadas (col, row) aproximam a forma do estado do Ceará — não são
 * geograficamente precisas, mas suficientes para um mapa simbólico hi-fi.
 * Para drill municipal real, trocar por TopoJSON do IBGE.
 */

export type RegiaoCE = {
  codigo: string;
  nome: string;
  col: number;
  row: number;
  /** Aglomerados litorâneos vs sertão — útil para anotações editoriais */
  zona: "litoral" | "metropolitana" | "sertao" | "cariri";
};

export const REGIOES_CE: RegiaoCE[] = [
  { codigo: "05", nome: "Litoral Norte",                  col: 2, row: 0, zona: "litoral" },
  { codigo: "03", nome: "Grande Fortaleza",               col: 3, row: 0, zona: "metropolitana" },
  { codigo: "04", nome: "Litoral Leste",                  col: 4, row: 0, zona: "litoral" },

  { codigo: "08", nome: "Serra da Ibiapaba",              col: 0, row: 1, zona: "sertao" },
  { codigo: "11", nome: "Sertão de Sobral",               col: 1, row: 1, zona: "sertao" },
  { codigo: "06", nome: "Litoral Oeste / Vale do Curu",   col: 2, row: 1, zona: "litoral" },
  { codigo: "07", nome: "Maciço do Baturité",             col: 3, row: 1, zona: "sertao" },
  { codigo: "14", nome: "Vale do Jaguaribe",              col: 4, row: 1, zona: "sertao" },

  { codigo: "12", nome: "Sertão dos Crateús",             col: 0, row: 2, zona: "sertao" },
  { codigo: "10", nome: "Sertão de Canindé",              col: 1, row: 2, zona: "sertao" },
  { codigo: "09", nome: "Sertão Central",                 col: 2, row: 2, zona: "sertao" },
  { codigo: "02", nome: "Centro Sul",                     col: 3, row: 2, zona: "sertao" },

  { codigo: "13", nome: "Sertão dos Inhamuns",            col: 0, row: 3, zona: "sertao" },
  { codigo: "01", nome: "Cariri",                         col: 3, row: 3, zona: "cariri" },
];

export const REGIOES_BY_CODIGO: Record<string, RegiaoCE> = Object.fromEntries(
  REGIOES_CE.map((r) => [r.codigo, r]),
);

export const GRID_COLS = 5;
export const GRID_ROWS = 4;

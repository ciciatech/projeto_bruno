/** Carregamento e tipos do painel regional CE. */

export type PainelRow = {
  r: string;       // regiao_codigo
  y: number;       // ano
  m: number;       // mes
  siof_emp?: number | null;
  siof_pago?: number | null;
  siof_dot?: number | null;
  siof_n?: number | null;
  ibcr?: number | null;
  inv_tot?: number | null;
  share?: number | null;
  // futuras colunas (BF, BPC, transf, CAGED, invest_municipal_siconfi, invest_federal):
  [k: string]: number | string | null | undefined;
};

export type PainelMeta = {
  regioes: { codigo: string; nome: string }[];
  linhas: number;
  periodo?: { inicio: string; fim: string };
  atualizado_em: string | null;
  fonte?: string;
  ausente?: boolean;
};

export type Painel = { meta: PainelMeta; rows: PainelRow[] };

let cache: Promise<Painel> | null = null;

export function carregarPainel(): Promise<Painel> {
  if (!cache) {
    cache = fetch("/data/painel.json", { cache: "no-cache" }).then((r) => {
      if (!r.ok) throw new Error(`Falha ao carregar painel: HTTP ${r.status}`);
      return r.json();
    });
  }
  return cache;
}

/** Última observação (ano/mês mais recente) por região, para snapshot/KPIs. */
export function snapshotPorRegiao(rows: PainelRow[]): Map<string, PainelRow> {
  const out = new Map<string, PainelRow>();
  for (const row of rows) {
    const cur = out.get(row.r);
    if (!cur || row.y > cur.y || (row.y === cur.y && row.m > cur.m)) {
      out.set(row.r, row);
    }
  }
  return out;
}

/** Soma um campo numérico (ignorando null/undefined). */
export function somar(rows: PainelRow[], campo: keyof PainelRow): number {
  let s = 0;
  for (const r of rows) {
    const v = r[campo];
    if (typeof v === "number" && !Number.isNaN(v)) s += v;
  }
  return s;
}

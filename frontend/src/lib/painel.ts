/** Carregamento e tipos do painel regional CE. */

export type PainelRow = {
  r: string;       // regiao_codigo
  y: number;       // ano
  m: number;       // mes
  // CAGED municipal (regional)
  adm?: number | null;       // admissões
  des?: number | null;       // desligamentos
  sal?: number | null;       // saldo (adm − des)
  mov?: number | null;       // total movimentações
  sal_med?: number | null;   // salário médio
  // Bolsa Família
  bf_v?: number | null;
  bf_b?: number | null;
  // BPC
  bpc_v?: number | null;
  bpc_b?: number | null;
  // transferências constitucionais STN federais
  tf_fpm?: number | null;
  tf_fundeb?: number | null;
  tf_itr?: number | null;
  tf_outros?: number | null;
  tf_royalties?: number | null;
  tf_total?: number | null;
  // SIOF (estadual, anual replicado em meses)
  siof_emp?: number | null;
  siof_pago?: number | null;
  siof_dot?: number | null;
  siof_n?: number | null;
  // invest. federal (Portal Transparência / RREO)
  if_direto?: number | null;
  if_ne?: number | null;
  if_nac?: number | null;
  if_total?: number | null;
  // macro estadual
  ibcr?: number | null;
  inv_tot?: number | null;
  share?: number | null;
  // população (anual replicado em meses)
  pop?: number | null;
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

type PainelIndex = {
  version: string;
  filename: string;
  atualizado_em: string | null;
  linhas: number;
};

/**
 * Carrega o painel em duas etapas:
 *   1. fetch /data/painel-index.json (pequeno, sem cache) para descobrir o
 *      filename hash-versionado do payload atual;
 *   2. fetch /data/painel.<hash>.json (cache imutável de 1 ano).
 *
 * Quando o painel é regenerado, o hash muda → o browser baixa o arquivo novo;
 * quando não muda, a segunda request bate em 304/cache local.
 *
 * Fallback: se o index falhar (build antigo / nginx sem regra), tenta
 * /data/painel.json direto.
 */
export function carregarPainel(): Promise<Painel> {
  if (!cache) {
    cache = (async () => {
      try {
        const idxResp = await fetch("/data/painel-index.json", { cache: "no-cache" });
        if (idxResp.ok) {
          const idx: PainelIndex = await idxResp.json();
          const dataResp = await fetch(`/data/${idx.filename}`);
          if (!dataResp.ok) {
            throw new Error(`Falha ao carregar ${idx.filename}: HTTP ${dataResp.status}`);
          }
          return await dataResp.json();
        }
      } catch (e) {
        console.warn("[painel] index.json indisponível, fallback para painel.json", e);
      }
      const r = await fetch("/data/painel.json", { cache: "no-cache" });
      if (!r.ok) throw new Error(`Falha ao carregar painel: HTTP ${r.status}`);
      return await r.json();
    })();
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

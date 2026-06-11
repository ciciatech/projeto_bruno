import { describe, expect, it } from "vitest";
import { somarAnualDedup, type PainelRow } from "./painel";

const row = (r: string, y: number, m: number, siof_obras: number | null): PainelRow => ({
  r,
  y,
  m,
  siof_obras,
});

describe("somarAnualDedup", () => {
  it("conta cada par (região, ano) uma única vez mesmo replicado em 12 meses", () => {
    const rows: PainelRow[] = [];
    for (let m = 1; m <= 12; m++) rows.push(row("03", 2024, m, 100));
    const agg = somarAnualDedup(rows, "siof_obras");
    expect(agg.total).toBe(100);
    expect(agg.porRegiao.get("03")).toBe(100);
    expect(agg.porAno.get(2024)).toBe(100);
  });

  it("soma regiões e anos distintos", () => {
    const rows = [
      row("01", 2024, 1, 10),
      row("01", 2024, 2, 10), // replicado — não conta de novo
      row("01", 2025, 1, 20),
      row("02", 2024, 1, 5),
    ];
    const agg = somarAnualDedup(rows, "siof_obras");
    expect(agg.total).toBe(35);
    expect(agg.porRegiao.get("01")).toBe(30);
    expect(agg.porRegiao.get("02")).toBe(5);
    expect(agg.porAno.get(2024)).toBe(15);
    expect(agg.porAno.get(2025)).toBe(20);
  });

  it("ignora null/NaN e usa o primeiro mês com valor", () => {
    const rows = [
      row("01", 2024, 1, null), // jan sem valor não 'queima' o ano
      row("01", 2024, 2, 42),
      row("01", 2024, 3, 42),
      row("02", 2024, 1, NaN),
    ];
    const agg = somarAnualDedup(rows, "siof_obras");
    expect(agg.total).toBe(42);
    expect(agg.porRegiao.has("02")).toBe(false);
  });

  it("campo ausente em todas as linhas devolve agregado vazio", () => {
    const agg = somarAnualDedup([row("01", 2024, 1, null)], "siof_equip");
    expect(agg.total).toBe(0);
    expect(agg.porAno.size).toBe(0);
  });
});

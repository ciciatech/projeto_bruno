import { describe, expect, it } from "vitest";
import { REGIOES_CE, REGIOES_BY_CODIGO, GRID_COLS, GRID_ROWS } from "./regioes";

describe("regioes_ce", () => {
  it("tem exatamente 14 regiões SEPLAG", () => {
    expect(REGIOES_CE).toHaveLength(14);
  });

  it("códigos vão de 01 a 14, sem buracos", () => {
    const codigos = REGIOES_CE.map((r) => r.codigo).sort();
    expect(codigos).toEqual([
      "01","02","03","04","05","06","07","08","09","10","11","12","13","14",
    ]);
  });

  it("todos os tiles cabem na grade declarada", () => {
    for (const r of REGIOES_CE) {
      expect(r.col).toBeGreaterThanOrEqual(0);
      expect(r.col).toBeLessThan(GRID_COLS);
      expect(r.row).toBeGreaterThanOrEqual(0);
      expect(r.row).toBeLessThan(GRID_ROWS);
    }
  });

  it("não há colisão de coordenadas (col, row)", () => {
    const seen = new Set<string>();
    for (const r of REGIOES_CE) {
      const key = `${r.col},${r.row}`;
      expect(seen.has(key)).toBe(false);
      seen.add(key);
    }
  });

  it("REGIOES_BY_CODIGO indexa corretamente", () => {
    expect(REGIOES_BY_CODIGO["03"].nome).toBe("Grande Fortaleza");
    expect(REGIOES_BY_CODIGO["01"].nome).toBe("Cariri");
  });

  it("toda região tem zona definida", () => {
    const zonas = new Set(["litoral", "metropolitana", "sertao", "cariri"]);
    for (const r of REGIOES_CE) {
      expect(zonas.has(r.zona)).toBe(true);
    }
  });
});

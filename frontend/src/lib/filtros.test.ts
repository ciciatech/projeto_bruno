import { describe, expect, it } from "vitest";
import { aplicarPeriodo, DEFAULT_FILTROS } from "./filtros";

describe("filtros", () => {
  describe("aplicarPeriodo", () => {
    const anos = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026];

    it("'Tudo' devolve a série completa", () => {
      expect(aplicarPeriodo(anos, "Tudo")).toEqual({ inicio: 2015, fim: 2026 });
    });

    it("'1A' devolve apenas o ano mais recente", () => {
      expect(aplicarPeriodo(anos, "1A")).toEqual({ inicio: 2026, fim: 2026 });
    });

    it("'5A' devolve os últimos 5 anos inclusivos", () => {
      expect(aplicarPeriodo(anos, "5A")).toEqual({ inicio: 2022, fim: 2026 });
    });

    it("clampa quando a série é mais curta que o recorte", () => {
      expect(aplicarPeriodo([2025, 2026], "10A")).toEqual({ inicio: 2025, fim: 2026 });
    });

    it("lista vazia devolve {0, 0}", () => {
      expect(aplicarPeriodo([], "3A")).toEqual({ inicio: 0, fim: 0 });
    });

    it("não depende da ordem da lista de anos", () => {
      expect(aplicarPeriodo([2026, 2015, 2020], "3A")).toEqual({ inicio: 2024, fim: 2026 });
    });
  });

  it("filtros padrão são Tudo/Bruto", () => {
    expect(DEFAULT_FILTROS).toEqual({ periodo: "Tudo", recorte: "Bruto" });
  });
});

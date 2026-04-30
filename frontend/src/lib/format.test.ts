import { describe, expect, it } from "vitest";
import { fmtBRL, fmtCompact, fmtNum, fmtPct, fmtMes } from "./format";

describe("format", () => {
  describe("fmtBRL", () => {
    it("formata bilhões", () => {
      expect(fmtBRL(1_500_000_000)).toBe("R$ 1,5 bi");
    });
    it("formata milhões", () => {
      expect(fmtBRL(2_000_000)).toBe("R$ 2 mi");
    });
    it("formata milhares", () => {
      expect(fmtBRL(50_000)).toBe("R$ 50 mil");
    });
    it("formata valores pequenos", () => {
      expect(fmtBRL(150)).toBe("R$ 150");
    });
    it("retorna — para NaN/null", () => {
      expect(fmtBRL(NaN)).toBe("—");
      expect(fmtBRL(null as unknown as number)).toBe("—");
    });
  });

  describe("fmtPct", () => {
    it("adiciona + para positivos", () => {
      expect(fmtPct(12.5)).toBe("+12,5%");
    });
    it("não adiciona + para negativos", () => {
      expect(fmtPct(-3.7)).toBe("-3,7%");
    });
    it("respeita dígitos", () => {
      expect(fmtPct(1.2345, 2)).toBe("+1,23%");
    });
  });

  describe("fmtNum", () => {
    it("usa separador BR", () => {
      expect(fmtNum(1234567)).toBe("1.234.567");
    });
  });

  describe("fmtCompact", () => {
    it("formata bilhões", () => {
      expect(fmtCompact(2_000_000_000)).toBe("2,0 bi");
    });
    it("formata milhões", () => {
      expect(fmtCompact(5_000_000)).toBe("5 mi");
    });
    it("formata milhares com sufixo k", () => {
      expect(fmtCompact(8_000)).toBe("8k");
    });
  });

  describe("fmtMes", () => {
    it("formata mês/ano abreviado", () => {
      expect(fmtMes(2024, 1)).toBe("jan/24");
      expect(fmtMes(2025, 12)).toBe("dez/25");
    });
  });
});

import { describe, expect, it } from "vitest";
import {
  ANO_SIOF_ESQUEMA_ANTIGO,
  ANO_SIOF_REGIONAL_INICIO,
  temCoberturaSiofRegional,
} from "./sioCobertura";

describe("sioCobertura", () => {
  it("série regional nas 14 regiões começa em 2016 (rel. 544)", () => {
    expect(ANO_SIOF_REGIONAL_INICIO).toBe(2016);
    expect(ANO_SIOF_ESQUEMA_ANTIGO).toBe(2015);
  });

  it("janela restrita a 2015 NÃO tem cobertura (esquema antigo, T47)", () => {
    expect(temCoberturaSiofRegional(2015, 2015)).toBe(false);
  });

  it("janela que alcança 2016+ tem cobertura", () => {
    expect(temCoberturaSiofRegional(2016, 2016)).toBe(true);
    expect(temCoberturaSiofRegional(2015, 2026)).toBe(true);
    expect(temCoberturaSiofRegional(2022, 2026)).toBe(true);
  });

  it("janela invertida não tem cobertura", () => {
    expect(temCoberturaSiofRegional(2026, 2016)).toBe(false);
  });
});

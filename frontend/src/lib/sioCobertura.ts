/**
 * SIOF-CE: cobertura regional (14 regiões SEPLAG/IPECE).
 *
 * Histórico da cobertura:
 *   - Antes (T≤45): só o ano corrente (2026) tinha desagregação regional —
 *     a extração corrente do SIOF-Web cobre apenas o exercício aberto.
 *   - Agora (T46/SCI-01): o relatório 544 do SIOF (famílias de relatório,
 *     filtro grupo 44, elementos 51=obras e 52=equipamentos) devolveu a
 *     série regional RETROATIVA 2016–2025, validada célula a célula contra
 *     a planilha do Prof. Paulo (806/806 obras e 656/656 equip dentro de
 *     ±1%). Com a extração corrente, a série regional cobre 2016+.
 *   - 2015 segue FORA: o SIOF de 2015 usa o esquema regional antigo
 *     (8 macrorregiões), incompatível com a grade atual de 14 regiões.
 *     A compatibilização está em tratamento (T47).
 *
 * Quando o frontend detecta período sem dado regional (hoje: janela restrita
 * a 2015 ou painel vazio), mostra um overlay editorial em vez de um mapa
 * todo da mesma cor (que dá falsa sensação de dado homogêneo).
 *
 * Fontes:
 *   - docs/decisao-sci01-siof-regional.md (mecânica do relatório 544)
 *   - docs/sci01-spike/siof_544_consolidado_2015_2025.csv (gabarito)
 *   - pipeline/transform/etl.py::processar_siof_obras
 */

/** Primeiro ano com desagregação regional na grade atual de 14 regiões. */
export const ANO_SIOF_REGIONAL_INICIO = 2016;

/** Ano com divisão regional antiga (8 macrorregiões), em tratamento (T47). */
export const ANO_SIOF_ESQUEMA_ANTIGO = 2015;

/**
 * Há cobertura SIOF regional em alguma parte da janela [anoInicio, anoFim]?
 * Falso apenas quando a janela inteira antecede 2016 (hoje: só 2015).
 */
export function temCoberturaSiofRegional(anoInicio: number, anoFim: number): boolean {
  return anoFim >= ANO_SIOF_REGIONAL_INICIO && anoFim >= anoInicio;
}

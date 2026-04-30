import Placeholder from "./Placeholder";

export default function Causal() {
  return (
    <Placeholder
      numero={4}
      titulo="Relação causal · investimento estadual → emprego"
      resumo="modelo econométrico da tese (DESP/UFC)"
      bullets={[
        "Scatter 14 regiões × meses · investimento (eixo X) vs saldo de empregos (eixo Y)",
        "Regressão OLS com IC 95% e teste de causalidade Granger",
        "Tabela β / α / R² / p-value · com controles regionais (BF, BPC, transferências, IBCR)",
        "Especificação econométrica e diagnósticos (heterocedasticidade, autocorrelação)",
      ]}
      fontes={
        "Tela final: roda só depois de Tela 1 (investimento) e Tela 2 (emprego) terem dados completos. Modelo segue a especificação aprovada pelo Prof. Paulo (abr/2026)."
      }
    />
  );
}

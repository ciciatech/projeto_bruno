import Placeholder from "./Placeholder";

export default function Emprego() {
  return (
    <Placeholder
      numero={2}
      titulo="Dinâmica do emprego formal · 14 regiões CE"
      resumo="CAGED municipal mensal + salário médio regional"
      bullets={[
        "Mapa divergente YoY: variação de saldo de empregos por região",
        "Linhas regionais sobrepostas: trajetória mensal 2015–2025",
        "Tabela densa: admissões, desligamentos, saldo, salário médio, por região",
        "Endogeneidade vs investimento estadual (modelo da tese)",
      ]}
      fontes={
        "CAGED municipal (FTP MTE) está implementado e roda como Onda 3 do watcher no Mac Mini (~12-24h). Salário médio regional é o cálculo do Paulo Ícaro, ainda em validação."
      }
    />
  );
}

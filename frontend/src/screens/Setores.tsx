import Placeholder from "./Placeholder";

export default function Setores() {
  return (
    <Placeholder
      numero={3}
      titulo="Composição setorial regional · CE"
      resumo="setores no PIB regional + LQ × tempo"
      bullets={[
        "Stacked bars: composição setorial (Indústria, Serviços, Agro, Constr., Comércio, Adm.) por região",
        "Heatmap LQ (location quotient): especialização setorial × região × ano",
        "Ranking de crescimento: setores em ascensão por região",
      ]}
      fontes={
        "Depende de coletas que ainda não estão no painel regional (PIB municipal IBGE, RAIS por CNAE setorial). Roadmap pós validação CAGED + SICONFI."
      }
    />
  );
}

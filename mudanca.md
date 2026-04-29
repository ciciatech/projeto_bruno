Mensagens Prof Paulo

Senhores, estou repensando os instrumentos de controle no exercício empírico da tese do Bruno. Nesse sentido, uma dúvida.
Será que conseguiríamos baixar automaticamente a base de dados chamada estban do banco central? Ela e.mensal


Outra dúvida. Nao me recordo se a base do bolsa família e BPC por município pode ser tb automatizada?


Respostas Cassio

Dois tipos de relatório, ambos gratuitos:
1. Relatório por município
Cada linha representa uma combinação de instituição financeira + município, com os saldos de todos os verbetes naquele município.
2. Relatório por município e agência
Detalha os mesmos dados mas ao nível de agência bancária individual.
Formato dos arquivos:

Disponibilizados como arquivos ZIP contendo CSV (texto separado por ponto e vírgula)
Arquivo mensal — abrange os últimos 6 meses publicados (atualmente: 07/2025 a 12/2025)
Defasagem de publicação: 60 dias após o mês de referência (dezembro: 90 dias)



Mensagens Prof Paulo

Sensacional

Possiveis endogenas regionais:
* emprego
* rendimento

Variáveis regionais de impacto:
* inv est obras
* inv est equip

Controle regional:
* Inv mun 
* inv fed
* inv priv
* credito estban
* BF
* BPC
* transferência estadual

Controle estadual:
* ibcr ce

Oi, meus caros. Vou mandar um áudio, tentarei ser objetivo.
Uma coisa que já estava pronta era o impacto do investimento do governo estadual numa determinada região como o Cariri — e isso foi feito para todas as 14 regiões. O foco era o investimento do governador naquela região, fosse em obras ou equipamentos, e como isso impactava os empregos daquela localidade.
Acontece que, além disso, todos os outros controles, como chamamos, eram de nível estadual, e não regionais. Ou seja, eram variáveis muito macro e não específicas de cada região. Os resultados já estavam prontos — inclusive cheguei a mostrar para vocês — impactando todos os salários nas 14 regiões. Na verdade, já estava pronto para o emprego e faltava apenas replicar para o salário nessas mesmas regiões.
Isso vinha me incomodando. Depois da reunião, me dei conta de que, com a expertise de vocês e a capacidade de extrair dados de forma automática e rápida, faz muito mais sentido buscar controles e instrumentos regionais. É justamente isso que está na mensagem objetiva que enviei agora.
As variáveis que queremos explicar em cada região continuam sendo:
•	Emprego: Já está pronto.
•	Rendimento: O Paulo Ícaro está evoluindo com a ajuda do Cássio.
•	Investimento do Governador (obras/equipamentos): Já está pronto por região.
E os novos controles regionais seriam:
•	Investimento dos municípios somados na região: Eu já tenho esses dados.
•	Investimento do Presidente na região: Não está pronto, mas os dados já foram extraídos do Excel. Com algumas fórmulas (como Procv), eu consigo resolver isso.
•	Investimento privado na região: Esse será um cálculo por exclusão. Pegamos o investimento total e subtraímos o que é do Estado, da Prefeitura e do Presidente; o que sobra é o investimento da sociedade. Também consigo fazer isso.
•	Crédito do BNB (Banco do Nordeste) por região: É viável, mas trabalhoso. Se automatizarmos a extração, fica simples. Manualmente, seria inviável processar 120 planilhas mensais.
•	Bolsa Família e BPC: Se tivermos os dados por cidade, agregamos e passamos a ter por região.
•	Transferência estadual: Mesma lógica: se houver por cidade, consolidamos por região.
O único indicador de controle que permanecerá puramente estadual (porque realmente não existe em nível regional) é o Índice de Atividade Econômica do Estado do Ceará. Vamos usá-lo para verificar se a região seguiu o mesmo padrão da atividade econômica do estado ao longo do tempo.
Teremos que refazer os 14 exercícios de emprego e fazer os de salário, mas isso não é um problema. Seja estimando por duas horas no MATLAB enquanto faço outra coisa, ou na rotina que o Cássio está desenvolvendo em Python, isso é o de menos.
O que importa é que agora teremos um exercício regionalmente muito mais controlado.
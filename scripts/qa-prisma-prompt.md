# QA visual · Prisma Regional CE

Cole o prompt abaixo numa sessão **Claude com browser tool** (Claude Chrome extension, Claude Desktop com computer use, ou outro agente que controle navegador). O agente vai navegar pelas rotas, capturar telas e reportar problemas.

> **Saída esperada**: ao final, o agente deve devolver os screenshots inline na conversa (ou indicar onde salvou). Cole as imagens de volta aqui no Claude Code que eu analiso e proponho correções.

---

## Prompt para colar

```
Você é um QA visual. Sua tarefa é navegar pelo aplicativo `https://prisma.bruno.ciciatech.cloud`
e produzir um relatório com screenshots e problemas encontrados.

Contexto: é um dashboard analítico (React 19 + Vite + Tailwind v4) sobre 14 regiões do
Ceará, com 5 telas. A linguagem visual usa paleta âmbar editorial inspirada no FT/Our
World in Data. Tipografia: Source Serif 4 (manchetes), Inter (UI), JetBrains Mono
(números). Header tem toggle claro/escuro.

PASSOS

1. Aceite o aviso de certificado se aparecer (Traefik default cert; o Let's Encrypt
   ainda pode estar emitindo). Confirme prosseguir.

2. Para cada rota abaixo, espere até 10s pelo carregamento completo, então capture
   um screenshot em viewport 1920×1080:
     a) /investimento (rota inicial — mapa coroplético + KPIs + composição)
     b) /emprego
     c) /setores
     d) /causal
     e) /pipeline

3. Em /investimento especificamente:
     - Capture um segundo screenshot APÓS clicar numa região do mapa diferente
       (ex: "Cariri" ou "Sertão Central"). Verifique se o painel direito
       atualiza com o nome e valor.
     - Capture um terceiro screenshot APÓS clicar no toggle "◐ escuro" no header,
       em modo dark.

4. Abra o DevTools (F12) → Console. Registre QUALQUER erro vermelho ou warning
   amarelo. Capture o painel do Console com os erros visíveis.

5. Abra DevTools → Network. Verifique:
     - /data/painel.json carrega com 200 OK e ~835 KB
     - /assets/index-*.js e index-*.css carregam com 200
     - Algum 404 ou request lento (>2s)?
   Capture o painel Network filtrado.

6. Verificações visuais a fazer (e reportar nos screenshots ou em texto):
     - O mapa coroplético tem 14 tiles distintos com números no rodapé de cada um?
     - Os KPIs do canto esquerdo mostram valores reais (não "—" nem "NaN")?
     - A tabela "Snapshot · 14 regiões" lista as 14 regiões e clicar nelas seleciona?
     - As fontes Source Serif 4 e JetBrains Mono carregaram (texto serifado nos
       títulos, monoespaçado nos números)?
     - Há overflow/clipping de texto em algum painel?
     - O header e a navegação `01 Investimento ... 05 Pipeline` aparecem corretos?
     - Em /pipeline a tabela das 11 fontes aparece com badges coloridos?

7. Reporte ao final:
     - "OK / NOK" por rota
     - Lista de bugs visuais (com severidade: crítico / médio / baixo)
     - Sugestões de melhoria de hierarquia ou densidade
     - URL final exibida no browser e código HTTP

ENTREGUE: todos os screenshots + texto do relatório. Se possível, salve as imagens
no formato `prisma-qa-NN-rota.png`.
```

---

## Ajustes opcionais

- Se quiser focar só na **Tela 1** (mais densa): troque o passo 2 por *"Captura só /investimento e /pipeline"*.
- Se o ambiente do agente não tiver DevTools acessível, remova passos 4–5; mantenha o restante.
- Se o agente não conseguir aceitar o cert default, peça pra ele acessar via `https://prisma.bruno.ciciatech.cloud` mesmo assim — o conteúdo carrega; é só o aviso visual que pode aparecer.

---

## Como me devolver

Quando o agente terminar, **copie-cole as imagens** (drag-and-drop ou paste direto) aqui no Claude Code. Eu analiso e abro PR com as correções.

"""
Constrói uma versão standalone do Prisma Regional Deck.

O HTML original (`Prisma_Regional_Deck.html`) referencia assets locais
(`tokens.css`, `*.jsx`, `deck-stage.js`) por path relativo. Streamlit serve
o componente em iframe sandboxed sem acesso aos arquivos vizinhos, então
inlinaremos tudo num único arquivo:

  Prisma_Regional_Deck.standalone.html

Uso:
    python3 dashboard/prisma-regional/build_standalone.py
"""

from __future__ import annotations

import re
from pathlib import Path

DIR = Path(__file__).resolve().parent
SRC = DIR / "Prisma_Regional_Deck.html"
DST = DIR / "Prisma_Regional_Deck.standalone.html"


def _read(name: str) -> str:
    return (DIR / name).read_text(encoding="utf-8")


def _inline_link_css(html: str) -> str:
    pattern = re.compile(r'<link\s+rel="stylesheet"\s+href="([^"]+\.css)"[^>]*/?>')

    def repl(m: re.Match) -> str:
        href = m.group(1)
        css = _read(href)
        return f"<style data-inlined-from='{href}'>\n{css}\n</style>"

    return pattern.sub(repl, html)


def _inline_scripts(html: str) -> str:
    """Substitui <script src="local.ext"> por <script>...content...</script>.

    Mantém scripts http/https intactos. Preserva atributos como type=text/babel.
    """
    pattern = re.compile(
        r'<script\s+([^>]*?)src="(?!https?:|//)([^"]+)"([^>]*)>\s*</script>'
    )

    def repl(m: re.Match) -> str:
        attrs_left = m.group(1).strip()
        src = m.group(2)
        attrs_right = m.group(3).strip()
        attrs = " ".join(p for p in (attrs_left, attrs_right) if p)
        body = _read(src)
        # remover atributo src=, manter os outros
        attrs = re.sub(r'\s*src="[^"]*"', "", attrs).strip()
        return f"<script {attrs} data-inlined-from='{src}'>\n{body}\n</script>"

    return pattern.sub(repl, html)


def main() -> None:
    html = SRC.read_text(encoding="utf-8")
    html = _inline_link_css(html)
    html = _inline_scripts(html)
    DST.write_text(html, encoding="utf-8")
    print(f"OK · {DST.relative_to(DIR.parent.parent)} · {DST.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()

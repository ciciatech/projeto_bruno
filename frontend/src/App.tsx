import { useEffect, useRef, useState } from "react";

const SLIDE_LABELS = [
  "Capa",
  "Paleta",
  "Tipografia",
  "Wireframes",
  "Componentes",
  "Racional",
  "Investimento",
  "Emprego",
  "Setores",
  "Causal",
  "Pipeline",
];

function App() {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [loaded, setLoaded] = useState(false);
  const [slideIndex, setSlideIndex] = useState(0);

  useEffect(() => {
    const onMsg = (e: MessageEvent) => {
      if (e.data && typeof e.data.slideIndexChanged === "number") {
        setSlideIndex(e.data.slideIndexChanged);
      }
    };
    window.addEventListener("message", onMsg);
    return () => window.removeEventListener("message", onMsg);
  }, []);

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-black">
      <iframe
        ref={iframeRef}
        title="Prisma Regional Deck"
        src="/deck.html"
        className="absolute inset-0 h-full w-full border-0"
        onLoad={() => setLoaded(true)}
        allow="fullscreen"
      />

      <div className="pointer-events-none absolute left-4 top-4 z-10 flex flex-col gap-1 font-mono text-[11px] uppercase tracking-wider text-white/60">
        <span>Prisma · Regional · IPEA · v0.4</span>
        {loaded && (
          <span className="text-white/40">
            Slide {String(slideIndex + 1).padStart(2, "0")}/{String(SLIDE_LABELS.length).padStart(2, "0")} ·{" "}
            {SLIDE_LABELS[slideIndex] ?? "—"} · ←/→ para navegar
          </span>
        )}
      </div>

      {!loaded && (
        <div className="absolute inset-0 flex items-center justify-center bg-[#14110b] text-white/70">
          <div className="font-mono text-sm tracking-wider">carregando deck…</div>
        </div>
      )}
    </div>
  );
}

export default App;

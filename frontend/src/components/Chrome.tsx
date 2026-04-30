import { NavLink } from "react-router-dom";
import { useEffect } from "react";

const TELAS = [
  { n: 1, slug: "investimento", label: "Investimento" },
  { n: 2, slug: "emprego",      label: "Emprego" },
  { n: 3, slug: "setores",      label: "Setores" },
  { n: 4, slug: "causal",       label: "Causal" },
  { n: 5, slug: "pipeline",     label: "Pipeline" },
];

export type Theme = "light" | "dark";

type Props = {
  theme: Theme;
  setTheme: (t: Theme) => void;
};

export function AppHeader({ theme, setTheme }: Props) {
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  return (
    <header
      className="flex items-center gap-6 border-b px-6"
      style={{
        height: 56,
        background: "var(--bg-surface)",
        borderColor: "var(--border-soft)",
        gridArea: "header",
      }}
    >
      <div className="flex items-baseline gap-2.5">
        <span
          className="serif"
          style={{ fontSize: 22, fontWeight: 600, letterSpacing: -0.4, color: "var(--ink-1)" }}
        >
          Prisma<span style={{ color: "var(--seq-3)" }}>·</span>Regional
        </span>
        <span
          style={{
            fontSize: 11,
            color: "var(--ink-3)",
            textTransform: "uppercase",
            letterSpacing: 0.8,
          }}
        >
          v1.0 · Tese DESP/UFC · 14 regiões CE
        </span>
      </div>

      <nav className="flex gap-0 ml-2">
        {TELAS.map((t) => (
          <NavLink
            key={t.slug}
            to={`/${t.slug}`}
            className={({ isActive }) =>
              "px-3 py-2 text-sm transition " +
              (isActive ? "font-semibold" : "font-medium opacity-70 hover:opacity-100")
            }
            style={({ isActive }) => ({
              color: isActive ? "var(--ink-1)" : "var(--ink-3)",
              borderBottom: isActive
                ? "2px solid var(--seq-3)"
                : "2px solid transparent",
              marginBottom: -1,
            })}
          >
            <span
              className="mono mr-1.5"
              style={{ fontSize: 10, color: "var(--ink-4)" }}
            >
              {String(t.n).padStart(2, "0")}
            </span>
            {t.label}
          </NavLink>
        ))}
      </nav>

      <div className="flex-1" />

      <div
        className="flex items-center gap-4"
        style={{ fontSize: 12, color: "var(--ink-3)" }}
      >
        <span className="flex items-center gap-1.5">
          <span
            className="inline-block"
            style={{
              width: 6,
              height: 6,
              borderRadius: 3,
              background: "var(--signal-good)",
            }}
          />
          Pipeline saudável
        </span>
        <span style={{ borderLeft: "1px solid var(--border-soft)", height: 16 }} />
        <button
          onClick={() => setTheme(theme === "light" ? "dark" : "light")}
          className="px-2.5 py-1 text-xs"
          style={{
            color: "var(--ink-2)",
            background: "var(--bg-surface)",
            border: "1px solid var(--border-strong)",
            borderRadius: "var(--radius-sm)",
            cursor: "pointer",
          }}
        >
          {theme === "light" ? "◐ escuro" : "◑ claro"}
        </button>
      </div>
    </header>
  );
}

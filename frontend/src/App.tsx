import { useState } from "react";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { AppHeader, type Theme } from "./components/Chrome";
import { FilterBar } from "./components/FilterBar";
import Investimento from "./screens/Investimento";
import Emprego from "./screens/Emprego";
import Setores from "./screens/Setores";
import Causal from "./screens/Causal";
import Pipeline from "./screens/Pipeline";

const THEME_KEY = "prisma-theme";

function App() {
  const [theme, setThemeState] = useState<Theme>(() => {
    if (typeof localStorage === "undefined") return "light";
    const saved = localStorage.getItem(THEME_KEY);
    return saved === "dark" || saved === "light" ? saved : "light";
  });
  const setTheme = (t: Theme) => {
    setThemeState(t);
    try {
      localStorage.setItem(THEME_KEY, t);
    } catch {
      // SSR/sandbox sem localStorage — ignorar
    }
  };

  return (
    <BrowserRouter>
      <Layout theme={theme} setTheme={setTheme}>
        <Routes>
          <Route path="/" element={<Navigate to="/investimento" replace />} />
          <Route path="/investimento" element={<Investimento />} />
          <Route path="/emprego" element={<Emprego />} />
          <Route path="/setores" element={<Setores />} />
          <Route path="/causal" element={<Causal />} />
          <Route path="/pipeline" element={<Pipeline />} />
          <Route path="*" element={<Navigate to="/investimento" replace />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}

/**
 * Layout exibe FilterBar somente em rotas analíticas — Pipeline não tem
 * filtros de período/recorte aplicáveis.
 */
function Layout({
  children,
  theme,
  setTheme,
}: {
  children: React.ReactNode;
  theme: Theme;
  setTheme: (t: Theme) => void;
}) {
  const { pathname } = useLocation();
  const showFilters = !pathname.startsWith("/pipeline");
  return (
    <div
      style={{
        display: "grid",
        gridTemplateRows: showFilters ? "56px auto 1fr" : "56px 1fr",
        gridTemplateAreas: showFilters
          ? "'header' 'filters' 'main'"
          : "'header' 'main'",
        minHeight: "100vh",
        background: "var(--bg-page)",
        color: "var(--ink-1)",
      }}
    >
      <AppHeader theme={theme} setTheme={setTheme} />
      {showFilters && <FilterBar />}
      {children}
    </div>
  );
}

export default App;

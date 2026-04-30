import { useState } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { AppHeader, type Theme } from "./components/Chrome";
import Investimento from "./screens/Investimento";
import Emprego from "./screens/Emprego";
import Setores from "./screens/Setores";
import Causal from "./screens/Causal";
import Pipeline from "./screens/Pipeline";

function App() {
  const [theme, setTheme] = useState<Theme>("light");

  return (
    <BrowserRouter>
      <div
        style={{
          display: "grid",
          gridTemplateRows: "56px 1fr",
          gridTemplateAreas: "'header' 'main'",
          minHeight: "100vh",
          background: "var(--bg-page)",
          color: "var(--ink-1)",
        }}
      >
        <AppHeader theme={theme} setTheme={setTheme} />
        <Routes>
          <Route path="/" element={<Navigate to="/investimento" replace />} />
          <Route path="/investimento" element={<Investimento />} />
          <Route path="/emprego" element={<Emprego />} />
          <Route path="/setores" element={<Setores />} />
          <Route path="/causal" element={<Causal />} />
          <Route path="/pipeline" element={<Pipeline />} />
          <Route path="*" element={<Navigate to="/investimento" replace />} />
        </Routes>
      </div>
    </BrowserRouter>
  );
}

export default App;

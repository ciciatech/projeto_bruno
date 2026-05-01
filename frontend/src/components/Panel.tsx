import type { ReactNode, CSSProperties } from "react";

type Props = {
  title?: string;
  eyebrow?: string;
  subtitle?: string;
  action?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
  padding?: number;
  style?: CSSProperties;
};

export function Panel({
  title,
  eyebrow,
  subtitle,
  action,
  footer,
  children,
  padding = 16,
  style,
}: Props) {
  return (
    <section
      className="flex flex-col min-w-0 min-h-0"
      style={{
        background: "var(--bg-surface)",
        border: "1px solid var(--border-soft)",
        ...style,
      }}
    >
      {(title || eyebrow) && (
        <header
          className="flex items-start gap-3"
          style={{
            padding: `${padding - 4}px ${padding}px ${padding - 8}px`,
            borderBottom: "1px solid var(--border-soft)",
          }}
        >
          <div className="flex-1 min-w-0">
            {eyebrow && (
              <div
                style={{
                  fontSize: 10,
                  color: "var(--ink-3)",
                  textTransform: "uppercase",
                  letterSpacing: 0.8,
                  fontWeight: 600,
                  marginBottom: 4,
                }}
              >
                {eyebrow}
              </div>
            )}
            {title && (
              <h2
                className="serif"
                style={{
                  margin: 0,
                  fontSize: 17,
                  fontWeight: 600,
                  color: "var(--ink-1)",
                  letterSpacing: -0.2,
                  lineHeight: 1.2,
                }}
              >
                {title}
              </h2>
            )}
            {subtitle && (
              <div
                style={{
                  fontSize: 12,
                  color: "var(--ink-3)",
                  marginTop: 4,
                  lineHeight: 1.4,
                }}
              >
                {subtitle}
              </div>
            )}
          </div>
          {action && <div className="shrink-0">{action}</div>}
        </header>
      )}
      <div
        style={{
          padding,
          flex: 1,
          minWidth: 0,
          minHeight: 0,
          // overflow auto permite que conteúdo grande role internamente
          // (em vez de ser cortado silenciosamente como `hidden` fazia).
          overflow: "auto",
        }}
      >
        {children}
      </div>
      {footer && (
        <footer
          className="flex items-center justify-between"
          style={{
            padding: `8px ${padding}px`,
            borderTop: "1px solid var(--border-soft)",
            fontSize: 11,
            color: "var(--ink-3)",
          }}
        >
          {footer}
        </footer>
      )}
    </section>
  );
}

export function EditorialNote({ children }: { children: ReactNode }) {
  return (
    <div
      style={{
        borderLeft: "2px solid var(--seq-3)",
        paddingLeft: 10,
        fontSize: 12,
        color: "var(--ink-2)",
        lineHeight: 1.5,
        fontStyle: "italic",
        fontFamily: "var(--font-serif)",
      }}
    >
      {children}
    </div>
  );
}

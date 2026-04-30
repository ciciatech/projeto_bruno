type Props = {
  data: number[];
  color?: string;
  width?: number;
  height?: number;
};

export function Sparkline({ data, color = "var(--seq-3)", width = 120, height = 20 }: Props) {
  if (!data || data.length < 2) return null;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const stepX = width / (data.length - 1);
  const points = data
    .map((v, i) => `${(i * stepX).toFixed(1)},${(height - ((v - min) / range) * height).toFixed(1)}`)
    .join(" ");

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} style={{ display: "block" }}>
      <polyline
        fill="none"
        stroke={color}
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
        points={points}
      />
    </svg>
  );
}

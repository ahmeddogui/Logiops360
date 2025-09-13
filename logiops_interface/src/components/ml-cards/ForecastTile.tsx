import React from "react";

export default function ForecastTile({ title, value, conf }: { title: string; value?: number | string; conf?: number }) {
  const pct = typeof conf === "number" ? (conf <= 1 ? Math.round(conf * 100) : Math.round(conf)) : null;
  return (
    <div className="rounded-2xl border p-4 bg-white">
      <div className="text-sm text-gray-500">{title}</div>
      <div className="text-2xl font-semibold mt-1">
        {value ?? "—"} <span className="text-sm font-normal text-gray-500">commandes</span>
      </div>
      {pct != null && <div className="text-xs text-gray-400 mt-1">Confiance: {pct}%</div>}
    </div>
  );
}

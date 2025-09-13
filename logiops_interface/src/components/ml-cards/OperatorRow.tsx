import React from "react";

export type OperatorItem = {
  name: string;
  zone: string;
  orders: number; // volume hebdo agrégé
  pct: number;    // ratio réel/prédit (0–100), utilisé en fallback
  status: "active" | "surcharge" | "sous-charge";
  barPct?: number; // largeur visuelle (0–100) normalisée sur le max des 'orders'
};

export default function OperatorRow({ item }: { item: OperatorItem }) {
  const badgeClass =
    item.status === "active"
      ? "bg-green-50 text-green-700"
      : item.status === "sous-charge"
      ? "bg-yellow-50 text-yellow-700"
      : "bg-red-50 text-red-700"; // surcharge

  // largeur de la barre : priorité à barPct (volume vs autres opérateurs), sinon pct (réel/prédit)
  const width = Math.max(0, Math.min(100, item.barPct ?? item.pct));

  return (
    <div>
      <div className="flex items-center justify-between text-sm">
        <div className="flex items-center gap-2">
          <span className="font-medium">{item.name}</span>
          <span className="px-2 py-0.5 rounded-full border text-xs">{item.zone}</span>
          <span className={`px-2 py-0.5 rounded-full text-xs ${badgeClass}`}>{item.status}</span>
        </div>
        <div className="text-gray-600">{item.orders} commandes</div>
      </div>
      <div className="mt-2 h-2 rounded-full bg-gray-100">
        <div className="h-2 rounded-full bg-gray-900" style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}

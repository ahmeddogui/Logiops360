// src/components/ml-cards/TopKpis.tsx
import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { TrendingUp } from "lucide-react";
import { getKpiCounters } from "@/services/ml";

type Props = { token: string };

export default function TopKpis({ token }: Props) {
  const [inProgress, setInProgress] = useState<number>(0);
  const [delayPct, setDelayPct] = useState<number>(0);   // % de retard (DelayRiskCard)
  const [anomCount, setAnomCount] = useState<number>(0); // nb anomalies (AnomalyP90Card)
  const activeCarriers = 5;

  // fetch KPI serveur (livraisons en cours)
  async function refreshCounters() {
    if (!token) return;
    try {
      const r = await getKpiCounters(token);
      setInProgress(Number(r?.in_progress ?? 0));
    } catch {
      // noop
    }
  }

  useEffect(() => { refreshCounters(); }, [token]);

  // écoute les events envoyés par DelayRiskCard / AnomalyP90Card
  useEffect(() => {
    function onDelayUpdate(e: Event) {
      const d = (e as CustomEvent)?.detail || {};
      const total = Number(d.total || 0);
      const late  = Number(d.late  || 0);
      const pct = total > 0 ? (late / total) * 100 : 0;
      setDelayPct(pct);
    }

    function onAnomUpdate(e: Event) {
      const d = (e as CustomEvent)?.detail || {};
      // Compat :
      // - nouveau format: { shipments: { total, byCount }, events: { total } }
      // - ancien format:  { total }
      const shipmentsTotal = Number(d?.shipments?.total ?? NaN);
      const eventsTotal    = Number(d?.events?.total ?? NaN);
      const flatTotal      = Number(d?.total ?? NaN);

      const val =
        Number.isFinite(shipmentsTotal) ? shipmentsTotal :
        Number.isFinite(eventsTotal)    ? eventsTotal :
        Number.isFinite(flatTotal)      ? flatTotal : 0;

      setAnomCount(val);
    }

    window.addEventListener("delay:list-updated", onDelayUpdate as EventListener);
    window.addEventListener("anom:list-updated",  onAnomUpdate  as EventListener);
    return () => {
      window.removeEventListener("delay:list-updated", onDelayUpdate as EventListener);
      window.removeEventListener("anom:list-updated",  onAnomUpdate  as EventListener);
    };
  }, []);

  const cards = [
    { title: "Livraisons en cours", value: String(inProgress),     change: "", colorClass: "text-primary" },
    { title: "Retards (affichés)",  value: `${delayPct.toFixed(1)}%`, change: "", colorClass: delayPct > 0 ? "text-destructive" : "text-success" },
    { title: "Transporteurs actifs", value: String(activeCarriers), change: "", colorClass: "text-primary" },
    { title: "Anomalies détectées", value: String(anomCount),      change: "", colorClass: anomCount > 0 ? "text-destructive" : "text-success" },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      {cards.map((c, i) => (
        <Card key={i} className="transition-all duration-300 hover:shadow-lg">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {c.title}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-primary">{c.value}</div>
            <div className={`text-xs flex items-center mt-1 ${c.colorClass}`}>
              <TrendingUp className="h-3 w-3 mr-1" />
              {c.change || "—"}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

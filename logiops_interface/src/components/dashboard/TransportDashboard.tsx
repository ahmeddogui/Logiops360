import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { BarChart3 } from "lucide-react";
import {
  ResponsiveContainer,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Bar,
} from "recharts";

import EtaPredictCard from "@/components/ml-cards/EtaPredictCard";
import DelayRiskCard from "@/components/ml-cards/DelayRiskCard";
import CarrierSimpleCard from "@/components/ml-cards/CarrierSimpleCard";
import AnomalyP90Card from "@/components/ml-cards/AnomalyP90Card";
import TopKpis from "@/components/ml-cards/TopKpis";

const API = (import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000/api").replace(/\/$/, "");

export const TransportDashboard = () => {
  const token = typeof window !== "undefined" ? localStorage.getItem("auth_token") ?? "" : "";
  const [charts, setCharts] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  function scrollToAnomalies() {
    const el = document.getElementById("anom-section");
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  const fetchCharts = async () => {
    try {
      const r = await fetch(`${API}/transport/charts?t=${Date.now()}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const j = await r.json();
      if (!r.ok) throw new Error(j?.error || "Erreur API");
      setCharts(j);
    } catch (e: any) {
      setError(e?.message || "Erreur inconnue");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchCharts();
  }, []);

  return (
    <div className="space-y-6 p-6">
      {/* En-tête */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-primary">Gestion du Transport</h1>
          <p className="text-muted-foreground">Suivi ETA et optimisation transporteurs</p>
        </div>
        <div>
          <Button size="sm" onClick={scrollToAnomalies}>
            <BarChart3 className="h-4 w-4 mr-2" />
            Analyser anomalies
          </Button>
        </div>
      </div>

      {/* KPIs dynamiques */}
      <TopKpis token={token} />

      {/* Ligne principale */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <DelayRiskCard token={token} />
        <div id="prediction-eta" className="flex flex-col gap-6">
          <CarrierSimpleCard token={token} />
          <EtaPredictCard token={token} />
        </div>
      </div>

      {/* Section anomalies */}
      <div id="anom-section">
        <AnomalyP90Card token={token} />
      </div>

      {/* Nouveaux graphiques transport */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Livraisons par Zone */}
<div className="rounded-2xl border bg-white p-4">
  <h2 className="text-lg font-semibold mb-2">Livraisons par Zone</h2>
  {loading ? (
    <p className="text-sm text-gray-500">Chargement…</p>
  ) : error ? (
    <p className="text-sm text-red-600">{error}</p>
  ) : (
    <ResponsiveContainer width="100%" height={250}>
      <BarChart data={charts?.deliveries_by_zone || []}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="destination_zone" />
        <YAxis />
        <Tooltip formatter={(val: any) => [val, "Livraisons"]} />
        <Bar dataKey="deliveries_count" fill="hsl(var(--primary))" />
      </BarChart>
    </ResponsiveContainer>
  )}
</div>


        {/* Frais moyens par transporteur */}
        <div className="rounded-2xl border bg-white p-4">
          <h2 className="text-lg font-semibold mb-2">Frais moyens par transporteur</h2>
          {loading ? (
            <p className="text-sm text-gray-500">Chargement…</p>
          ) : error ? (
            <p className="text-sm text-red-600">{error}</p>
          ) : (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={charts?.avg_cost_by_carrier || []}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="carrier" />
                <YAxis />
                <Tooltip formatter={(val: any) => [`${val} €`, "Coût moyen"]} />
                <Bar dataKey="avg_cost" fill="hsl(var(--secondary))" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  );
};

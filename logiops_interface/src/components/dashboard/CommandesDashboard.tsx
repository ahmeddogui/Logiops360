//Dashbord commande
import { useEffect, useState } from "react";
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  LineChart,
  Line,
  ComposedChart
} from "recharts";
import KpiCard from "../ml-cards/KpiCard";
import ForecastTile from "../ml-cards/ForecastTile";
import OperatorRow, { OperatorItem } from "../ml-cards/OperatorRow";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";

const API = (import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000/api").replace(/\/$/, "");

type Tile = { orders?: number; confidence?: number };
type ForecastResponse = {
  today?: Tile;
  tomorrow?: Tile;
  this_week?: Tile;
  this_month?: Tile;
  metadata?: { model_version?: number | string; trained_at?: string; snapshot_at?: string };
};

export default function CommandesDashboard() {
  const [forecast, setForecast] = useState<ForecastResponse | null>(null);
  const [kpi, setKpi] = useState<{ day_orders?: number; week_orders?: number; avg_operator_load?: number }>({});
  const [ops, setOps] = useState<OperatorItem[]>([]);
  const [file, setFile] = useState<File | null>(null);
  const [isTraining, setIsTraining] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [chartData, setChartData] = useState<any>(null);

  // ---------- API helpers ----------
  const fetchForecast = async () => {
    const r = await fetch(`${API}/forecast?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /forecast ${r.status}`);
    return j as ForecastResponse;
  };

  const fetchKpi = async () => {
    const r = await fetch(`${API}/kpi/orders_summary?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /kpi/orders_summary ${r.status}`);
    return j as { day_orders?: number; week_orders?: number; avg_operator_load?: number };
  };

  const fetchOps = async () => {
    const r = await fetch(`${API}/operators/load_status?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /operators/load_status ${r.status}`);
    return (j.items ?? []) as OperatorItem[];
  };

  const fetchCharts = async () => {
    const r = await fetch(`${API}/supervisor/charts?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /supervisor/charts ${r.status}`);
    return j;
  };

  const uploadAndRetrain = async (f: File) => {
    const fd = new FormData();
    fd.append("file", f, f.name);

    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 15 * 60 * 1000);

    try {
      const resp = await fetch(`${API}/orders/upload`, {
        method: "POST",
        body: fd,
        headers: { "Cache-Control": "no-store" },
        signal: ctrl.signal,
      });
      clearTimeout(t);
      const j = await resp.json();
      if (!resp.ok) throw new Error(j?.error || `POST /orders/upload ${resp.status}`);
      return j;
    } catch (e) {
      throw e;
    }
  };

  const refreshAll = async () => {
    setLoading(true);
    setError(null);
    try {
      const [f, k, o, c] = await Promise.all([fetchForecast(), fetchKpi(), fetchOps(), fetchCharts()]);

      const maxOrders = Math.max(1, ...o.map((x) => x?.orders ?? 0));
      const oScaled: OperatorItem[] = o.map((x) => ({
        ...x,
        barPct: Math.round(((x?.orders ?? 0) / maxOrders) * 100),
      }));

      setForecast(f);
      setKpi(k);
      setOps(oScaled);
      setChartData(c);
    } catch (e: any) {
      setError(e?.message || "Erreur inconnue");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshAll();
  }, []);

  const handleRetrain = async () => {
    if (!file) return;
    setIsTraining(true);
    setError(null);
    try {
      await uploadAndRetrain(file);
      await refreshAll();
    } catch (e: any) {
      setError(e?.message || "Réentraînement impossible");
    } finally {
      setIsTraining(false);
    }
  };

  const fmt = (n?: number) => (typeof n === "number" ? n.toLocaleString("fr-FR") : "—");

  const overloadCount = ops.filter((o) => o.status === "surcharge").length;
  const overloadPct = ops.length ? Math.round((overloadCount / ops.length) * 100) : 0;
  const overloadSub = ops.length ? `${overloadCount}/${ops.length} opérateurs` : undefined;

  return (
    <div className="space-y-6">
      {/* En-tête */}
      <section className="rounded-2xl border bg-white">
        <div className="p-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h1 className="text-3xl font-bold text-primary">Gestion des Commandes</h1>
            <p className="text-muted-foreground">Analyse et prévision des volumes de commandes</p>
          </div>

          <div className="flex flex-col sm:flex-row gap-2 sm:items-center">
            <button
              onClick={refreshAll}
              className="px-3 py-1.5 rounded-md border hover:bg-gray-50"
              disabled={loading}
            >
              Lancer une nouvelle prévision
            </button>

            <div className="flex items-center gap-2">
              <input
                type="file"
                accept=".csv,text/csv"
                onChange={(e) => setFile(e.target.files?.[0] ?? null)}
                className="text-sm"
              />
              <button
                onClick={handleRetrain}
                disabled={!file || isTraining}
                className="px-3 py-1.5 rounded-md border hover:bg-gray-50 disabled:opacity-50"
              >
                {isTraining ? "Réentraînement..." : "Réentraîner"}
              </button>
            </div>
          </div>
        </div>

        {/* KPIs */}
        <div className="border-t p-4">
          {loading ? (
            <div className="text-sm text-gray-500">Chargement…</div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <KpiCard title="Commandes du jour" value={fmt(kpi.day_orders)} />
              <KpiCard title="Commandes de la semaine" value={fmt(kpi.week_orders)} />
              <KpiCard title="Opérateurs en surcharge" value={`${overloadPct}%`} sub={overloadSub} />
              <KpiCard title="Temps moyen picking" value="12.4 min" sub="(dernière mesure)" />
            </div>
          )}
          {error && <div className="mt-3 text-sm text-red-600">{error}</div>}
        </div>
      </section>

      {/* Colonnes existantes */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Charge opérateurs */}
        <div className="rounded-2xl border bg-white">
          <div className="p-4">
            <div className="text-sm text-gray-500 mb-1">Charge des opérateurs</div>
            <div className="text-xs text-gray-400 mb-3">
              Comparaison réel vs prédit (réel &lt; prédit ⇒ surcharge)
            </div>
            {loading ? (
              <div className="text-sm text-gray-500">Chargement…</div>
            ) : ops.length === 0 ? (
              <div className="text-sm text-gray-500">Aucun opérateur trouvé pour la période.</div>
            ) : (
              <div className="space-y-4">
                {ops.map((it, idx) => (
                  <OperatorRow key={`${it.name}-${idx}`} item={it} />
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Prévisions */}
        <div className="rounded-2xl border bg-white">
          <div className="p-4">
            <div className="text-sm text-gray-500 mb-3">Prévisions</div>
            {loading ? (
              <div className="text-sm text-gray-500">Chargement…</div>
            ) : (
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                <ForecastTile title="Aujourd’hui" value={forecast?.today?.orders} conf={forecast?.today?.confidence} />
                <ForecastTile title="Demain" value={forecast?.tomorrow?.orders} conf={forecast?.tomorrow?.confidence} />
                <ForecastTile title="Cette semaine" value={forecast?.this_week?.orders} conf={forecast?.this_week?.confidence} />
                <ForecastTile title="Ce mois" value={forecast?.this_month?.orders} conf={forecast?.this_month?.confidence} />
              </div>
            )}
          </div>
        </div>
      </section>

      {/* === Graphiques de performance (PLACÉS EN BAS) === */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Performance des opérateurs */}
        <Card>
          <CardHeader>
            <CardTitle>Performance opérateurs (aujourd'hui)</CardTitle>
            <CardDescription>Top 15 opérateurs par nombre de commandes traitées</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData?.operator_performance || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="operator"
                    tick={{ fontSize: 10 }}
                    interval={0}
                    angle={-45}
                    textAnchor="end"
                  />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip
                    formatter={(value: any, name: string) => {
                      const labels: Record<string, string> = {
                        orders_processed: "Commandes",
                        total_units: "Unités totales",
                        waves_handled: "Vagues gérées",
                      };
                      return [value, labels[name] || name];
                    }}
                  />
                  <Bar dataKey="orders_processed" fill="hsl(var(--primary))" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        {/* Évolution des commandes */}
        <Card>
          <CardHeader>
            <CardTitle>Évolution des commandes (7 jours)</CardTitle>
            <CardDescription>Volume quotidien et nombre d'opérateurs</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData?.orders_trend || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    dataKey="date" 
                    tick={{ fontSize: 12 }}
                    tickFormatter={(value) => new Date(value).toLocaleDateString('fr-FR', { month: 'short', day: 'numeric' })}
                  />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip 
                    labelFormatter={(value) => new Date(value).toLocaleDateString('fr-FR')}
                    formatter={(value: any, name: string) => [
                      value,
                      name === 'orders_count' ? 'Commandes' : 'Opérateurs'
                    ]}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="orders_count" 
                    stroke="hsl(var(--primary))" 
                    strokeWidth={2}
                    dot={{ fill: "hsl(var(--primary))", r: 4 }}
                  />
                  <Line 
                    type="monotone" 
                    dataKey="operators_count" 
                    stroke="hsl(var(--secondary))" 
                    strokeWidth={2}
                    dot={{ fill: "hsl(var(--secondary))", r: 4 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        
       
      </section>
    </div>
  );
}

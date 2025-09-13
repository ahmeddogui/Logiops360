import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import {
  TrendingUp,
  Package,
  Warehouse,
  Truck,
  Users,
  AlertTriangle,
  Bot,
  Zap,
} from "lucide-react";

/* === Recharts === */
import {
  ResponsiveContainer,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Bar,
  ComposedChart,
  Line,
} from "recharts";

/* ================= Config ================= */
const API = (import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000/api").replace(/\/$/, "");

/* ================= Types ================= */
type ViewKey = "dashboard" | "commandes" | "stockage" | "transport";
type Props = {
  onNavigate?: (view: ViewKey, anchor?: string) => void;
};

/* ================= Utils ================= */
const authHeaders = (): Record<string, string> => {
  try {
    const raw =
      localStorage.getItem("token") ||
      localStorage.getItem("access_token") ||
      localStorage.getItem("auth_token") ||
      "";
    if (!raw) return {};
    return { Authorization: raw.startsWith("Bearer ") ? raw : `Bearer ${raw}` };
  } catch {
    return {};
  }
};

async function safeJsonGet(url: string, withAuth = false): Promise<any | null> {
  try {
    const headers: Record<string, string> = {};
    if (withAuth) Object.assign(headers, authHeaders());
    const resp = await fetch(url, {
      method: "GET",
      headers,
      credentials: "include",
      cache: "no-store",
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

/** ===== Formatage dates pour l’axe/tooltip ===== */
function fmtDayShort(v: any) {
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  // ex: "11 sept"
  return d
    .toLocaleDateString("fr-FR", { day: "2-digit", month: "short" })
    .replace(".", "");
}
function fmtDayLong(v: any) {
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return String(v);
  // ex: "11 septembre 2025"
  return d.toLocaleDateString("fr-FR", { day: "2-digit", month: "long", year: "numeric" });
}

/* ================= Component ================= */
const SupervisorDashboard = ({ onNavigate }: Props) => {
  // KPI top
  const [kpiTop, setKpiTop] = useState({
    distinctSkus: "—",
    occupancyRate: "—",
    deliveries: "—",
    operators: "—",
  });

  // Bloc alertes
  const [latePct, setLatePct] = useState<number>(0);
  const [anomCount, setAnomCount] = useState<number>(0);
  const [zonesCritical, setZonesCritical] = useState<number>(0);
  const [locsAttention, setLocsAttention] = useState<number>(0);
  const [ordersWeek, setOrdersWeek] = useState<number>(0);
  const [loadingAlerts, setLoadingAlerts] = useState<boolean>(false);
  const [errAlerts, setErrAlerts] = useState<string | null>(null);

  // Graphiques consolidés
  const [charts, setCharts] = useState<any>(null);

  // ---- Navigation via le parent (Index.tsx)
  const go = (view: ViewKey, anchor?: string) => {
    if (onNavigate) onNavigate(view, anchor);
  };

  // ---- Charge KPI top
  const refreshTopKpis = async () => {
    const k = await safeJsonGet(`${API}/storage/kpis?t=${Date.now()}`);
    const distinctSkus =
      typeof k?.distinct_skus === "number" ? k.distinct_skus.toLocaleString("fr-FR") : "—";
    const occupancyRate =
      typeof k?.occupancy_rate === "number" ? `${Math.round(k.occupancy_rate * 100)}%` : "—";
    const saturatedPct =
      typeof k?.saturated_locations_pct === "number"
        ? `${Math.round(k.saturated_locations_pct * 100)}%`
        : "—";

    const ops = await safeJsonGet(`${API}/operators/load_status?t=${Date.now()}`);
    let overloadStr = "—";
    if (ops && Array.isArray(ops.items)) {
      const total = ops.items.length;
      if (total > 0) {
        const overloadCount = ops.items.filter(
          (o: any) => String(o?.status || "").toLowerCase() === "surcharge"
        ).length;
        const pct = Math.round((overloadCount / total) * 100);
        overloadStr = `${pct}%`;
      } else {
        overloadStr = "0%";
      }
    }

    setKpiTop({
      distinctSkus,
      occupancyRate,
      deliveries: saturatedPct,
      operators: overloadStr,
    });
  };

  // ---- Charge alertes
  const refreshAlerts = async () => {
    setLoadingAlerts(true);
    setErrAlerts(null);
    try {
      const eta = await safeJsonGet(`${API}/ml/eta/shipments?t=${Date.now()}`, true);
      let latePctVal = 0;
      if (eta && (Array.isArray(eta.items) || Array.isArray(eta.data))) {
        const rows: any[] = eta.items ?? eta.data ?? [];
        const n = rows.length;
        if (n > 0) {
          const late = rows.filter(
            (r) =>
              r?.is_late === true ||
              String(r?.status || "").toLowerCase() === "late" ||
              (typeof r?.delay_min === "number" && r.delay_min > 0) ||
              (typeof r?.delay_minutes === "number" && r.delay_minutes > 0)
          ).length;
          latePctVal = Math.round((late / n) * 100);
        }
      }

      const anom = await safeJsonGet(`${API}/ml/anom/list?t=${Date.now()}`, true);
      const anomCountVal = anom && Array.isArray(anom.items) ? anom.items.length : 0;

      const zones = await safeJsonGet(`${API}/storage/zones/occupancy?t=${Date.now()}`);
      const zonesCritVal =
        zones && Array.isArray(zones.items)
          ? zones.items.filter((it: any) => String(it?.status || "").toLowerCase() === "critique").length
          : 0;

      const hot = await safeJsonGet(`${API}/storage/hotspots?t=${Date.now()}`);
      const locsAttVal = hot && Array.isArray(hot.items) ? hot.items.length : 0;

      const kpi = await safeJsonGet(`${API}/kpi/orders_summary?t=${Date.now()}`);
      const week = kpi?.week_orders ?? 0;

      setLatePct(latePctVal);
      setAnomCount(anomCountVal);
      setZonesCritical(zonesCritVal);
      setLocsAttention(locsAttVal);
      setOrdersWeek(week);
    } catch (e: any) {
      setErrAlerts(e?.message || "Erreur lors du chargement des alertes");
    } finally {
      setLoadingAlerts(false);
    }
  };

  // ---- Charge graphiques
  const refreshCharts = async () => {
    const sup = await safeJsonGet(`${API}/supervisor/charts?t=${Date.now()}`);
    const storage = await safeJsonGet(`${API}/storage/analytics?t=${Date.now()}`);
    const transport = await safeJsonGet(`${API}/transport/charts?t=${Date.now()}`);
    setCharts({
      ...sup,
      ...storage,
      ...transport,
    });
  };

  useEffect(() => {
    refreshTopKpis();
    refreshAlerts();
    refreshCharts();
  }, []);

  // Cartes KPI en haut
  const kpiCards = [
    { title: "Produits différents", value: kpiTop.distinctSkus, icon: Package, color: "text-success" },
    { title: "Taux de remplissage", value: kpiTop.occupancyRate, icon: Warehouse, color: "text-success" },
    { title: "Emplacements saturés", value: kpiTop.deliveries, icon: Truck, color: "text-warning" },
    { title: "% opérateurs en surcharge", value: kpiTop.operators, icon: Users, color: "text-primary" },
  ];

  const aiModels = [
    { name: "Prévision de demande", status: "active", onClick: () => go("commandes") },
    { name: "Optimisation slotting", status: "active", onClick: () => go("stockage", "top5-produits-in-out") },
    { name: "Prédiction ETA", status: "active", onClick: () => go("transport", "eta-section") },
    { name: "Détection d'anomalies", status: "active", onClick: () => go("transport", "anom-section") },
  ];

  const alerts = useMemo(
    () => [
      {
        type: anomCount > 0 ? "error" : "info",
        label: `${anomCount} anomalies détectées sur le réseau transport`,
        sub: "Modèle de détection d'anomalies",
        onClick: () => go("transport", "anom-section"),
      },
      {
        type: zonesCritical > 0 ? "error" : "success",
        label: `${zonesCritical} zone(s) en statut critique dans l'entrepôt`,
        sub: "Occupation par zones",
        onClick: () => go("stockage", "top5-produits-in-out"),
      },
      {
        type: locsAttention > 0 ? "warning" : "info",
        label: `${locsAttention} emplacement(s) nécessitent attention`,
        sub: "Hotspots stockage",
        onClick: () => go("stockage", "top5-produits-in-out"),
      },
      {
        type: "info",
        label: `${ordersWeek.toLocaleString("fr-FR")} commandes prévues cette semaine`,
        sub: "Prévisions & KPI commandes",
        onClick: () => go("commandes"),
      },
    ],
    [anomCount, zonesCritical, locsAttention, ordersWeek]
  );

  const dotClass = (t: string) =>
    t === "error" ? "bg-destructive" : t === "warning" ? "bg-yellow-500" : t === "success" ? "bg-emerald-500" : "bg-secondary";

  return (
    <div className="space-y-6 p-6">
      {/* En-tête */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-primary">Dashboard Superviseur</h1>
          <p className="text-muted-foreground">Vue d'ensemble de la chaîne logistique</p>
        </div>
        <div className="flex space-x-2">
          <Button size="sm" onClick={() => { refreshTopKpis(); refreshAlerts(); refreshCharts(); }} disabled={loadingAlerts}>
            <Zap className="h-4 w-4 mr-2" />
            Actualiser
          </Button>
        </div>
      </div>

      {/* KPI TOP */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {kpiCards.map((kpi, index) => (
          <Card key={index} className="transition-all duration-300 hover:shadow-lg">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">{kpi.title}</CardTitle>
              <kpi.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-primary">{kpi.value}</div>
              <div className={`text-xs flex items-center mt-1 ${kpi.color}`}>
                <TrendingUp className="h-3 w-3 mr-1" />
                —
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Modèles IA */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Bot className="h-5 w-5" />
              <span>Modèles IA</span>
            </CardTitle>
            <CardDescription>Déclenchement et monitoring des algorithmes</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {aiModels.map((m, i) => (
              <div key={i} className="flex items-center justify-between p-3 border rounded-lg">
                <div className="flex-1">
                  <div className="flex items-center space-x-2">
                    <h4 className="font-medium">{m.name}</h4>
                    <Badge variant={m.status === "active" ? "default" : m.status === "alert" ? "destructive" : "secondary"}>
                      {m.status}
                    </Badge>
                  </div>
                  <div className="text-sm text-muted-foreground mt-1">
                    Précision: {m.accuracy} • {m.lastRun}
                  </div>
                  <Progress value={parseFloat(m.accuracy)} className="mt-2 h-2" />
                </div>
                <Button size="sm" className="ml-4" onClick={m.onClick}>
                  Détails
                </Button>
              </div>
            ))}
          </CardContent>
        </Card>

        {/* Alertes dynamiques */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <AlertTriangle className="h-5 w-5" />
              <span>Alertes en temps réel</span>
            </CardTitle>
            <CardDescription>Notifications importantes de la supply chain</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {loadingAlerts && <div className="text-sm text-muted-foreground">Chargement des alertes…</div>}
            {errAlerts && <div className="text-sm text-red-600">{errAlerts}</div>}
            {!loadingAlerts &&
              !errAlerts &&
              alerts.map((a, idx) => (
                <button
                  key={idx}
                  onClick={a.onClick}
                  className="w-full text-left p-3 border rounded-lg hover:bg-muted/40 transition-colors"
                >
                  <div className="flex items-start space-x-3">
                    <div className={`w-2 h-2 rounded-full mt-2 ${dotClass(a.type)}`} />
                    <div className="flex-1">
                      <p className="text-sm font-medium">{a.label}</p>
                      <p className="text-xs text-muted-foreground">{a.sub}</p>
                    </div>
                  </div>
                </button>
              ))}
          </CardContent>
        </Card>
      </div>

      {/* Performance de la chaîne logistique */}
      <Card>
        <CardHeader>
          <CardTitle>Performance de la chaîne logistique</CardTitle>
          <CardDescription>Métriques clés sur les dernières 24h</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Commandes - évolution 7j */}
            <div className="rounded-2xl border bg-white p-4">
              <h3 className="text-base font-semibold mb-2">Évolution des commandes (7 jours)</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={charts?.orders_trend || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  {/* ✅ Formatage des dates */}
                  <XAxis
                    dataKey="date"
                    tickFormatter={fmtDayShort}
                    interval={0}
                    angle={-20}
                    textAnchor="end"
                    height={40}
                  />
                  <YAxis />
                  <Tooltip
                    labelFormatter={fmtDayLong}
                    formatter={(val: any, name: any) => [val, name === "orders_count" ? "Commandes" : name]}
                  />
                  <Bar dataKey="orders_count" fill="hsl(var(--primary))" />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Stockage - Répartition classes */}
            <div className="rounded-2xl border bg-white p-4">
              <h3 className="text-base font-semibold mb-2">Répartition produits par classe</h3>
              <ResponsiveContainer width="100%" height={250}>
                <ComposedChart data={charts?.class_distribution || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="class" />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Bar yAxisId="left" dataKey="nb_products" fill="hsl(var(--primary))" />
                  <Line yAxisId="right" dataKey="total_qty" stroke="hsl(var(--secondary))" />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            {/* Transport - Livraisons par zone */}
            <div className="rounded-2xl border bg-white p-4">
              <h3 className="text-base font-semibold mb-2">Livraisons par Zone</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={charts?.deliveries_by_zone || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="destination_zone" />
                  <YAxis />
                  <Tooltip />
                  <Bar dataKey="deliveries_count" fill="hsl(var(--primary))" />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Transport - Frais moyens */}
            <div className="rounded-2xl border bg-white p-4">
              <h3 className="text-base font-semibold mb-2">Frais moyens par transporteur</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={charts?.avg_cost_by_carrier || []}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="carrier" />
                  <YAxis />
                  <Tooltip formatter={(val: any) => [`${val} €`, "Coût moyen"]} />
                  <Bar dataKey="avg_cost" fill="hsl(var(--secondary))" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default SupervisorDashboard;
export { SupervisorDashboard };

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
  BarChart3,
  Zap,
} from "lucide-react";

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

/* ================= Component ================= */
const SupervisorDashboard = ({ onNavigate }: Props) => {
  // KPI top
  const [kpiTop, setKpiTop] = useState({
    distinctSkus: "—",
    occupancyRate: "—",
    deliveries: "—", // <- réutilisé pour "Emplacements saturés"
    operators: "—",  // <- réutilisé pour "% opérateurs en surcharge"
  });

  // Bloc alertes
  const [latePct, setLatePct] = useState<number>(0);
  const [anomCount, setAnomCount] = useState<number>(0);
  const [zonesCritical, setZonesCritical] = useState<number>(0);
  const [locsAttention, setLocsAttention] = useState<number>(0);
  const [ordersWeek, setOrdersWeek] = useState<number>(0);
  const [loadingAlerts, setLoadingAlerts] = useState<boolean>(false);
  const [errAlerts, setErrAlerts] = useState<string | null>(null);

  // ---- Navigation via le parent (Index.tsx)
  const go = (view: ViewKey, anchor?: string) => {
    if (onNavigate) onNavigate(view, anchor);
  };

  // ---- Charge KPI top
  const refreshTopKpis = async () => {
    // Stockage KPIs
    const k = await safeJsonGet(`${API}/storage/kpis?t=${Date.now()}`);
    const distinctSkus =
      typeof k?.distinct_skus === "number" ? k.distinct_skus.toLocaleString("fr-FR") : "—";
    const occupancyRate =
      typeof k?.occupancy_rate === "number" ? `${Math.round(k.occupancy_rate * 100)}%` : "—";

    // Emplacements saturés (depuis stockage KPIs)
    const saturatedPct =
      typeof k?.saturated_locations_pct === "number"
        ? `${Math.round(k.saturated_locations_pct * 100)}%`
        : "—";

    // % opérateurs en surcharge (depuis /operators/load_status comme le dashboard Commandes)
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

    // NOTE: on réutilise 'deliveries' et 'operators' pour ne pas impacter le reste du composant.
    setKpiTop({
      distinctSkus,
      occupancyRate,
      deliveries: saturatedPct, // <- "Emplacements saturés"
      operators: overloadStr,   // <- "% opérateurs en surcharge"
    });
  };

  // ---- Charge alertes (inchangé)
  const refreshAlerts = async () => {
    setLoadingAlerts(true);
    setErrAlerts(null);
    try {
      // Transport — ETA (% retard)
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

      // Transport — Anomalies
      const anom = await safeJsonGet(`${API}/ml/anom/list?t=${Date.now()}`, true);
      const anomCountVal = anom && Array.isArray(anom.items) ? anom.items.length : 0;

      // Stockage — Zones critiques
      const zones = await safeJsonGet(`${API}/storage/zones/occupancy?t=${Date.now()}`);
      const zonesCritVal =
        zones && Array.isArray(zones.items)
          ? zones.items.filter((it: any) => String(it?.status || "").toLowerCase() === "critique").length
          : 0;

      // Stockage — Hotspots
      const hot = await safeJsonGet(`${API}/storage/hotspots?t=${Date.now()}`);
      const locsAttVal = hot && Array.isArray(hot.items) ? hot.items.length : 0;

      // Commandes — KPI semaine
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

  useEffect(() => {
    refreshTopKpis();
    refreshAlerts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Cartes KPI en haut (seuls les libellés des 2 cartes changent)
  const kpiCards = [
    { title: "Produits différents", value: kpiTop.distinctSkus, icon: Package, color: "text-success" },
    { title: "Taux de remplissage", value: kpiTop.occupancyRate, icon: Warehouse, color: "text-success" },
    { title: "Emplacements saturés", value: kpiTop.deliveries, icon: Truck, color: "text-warning" },
    { title: "% opérateurs en surcharge", value: kpiTop.operators, icon: Users, color: "text-primary" },
  ];

  // Liste modèles IA (inchangé)
  const aiModels = [
    { name: "Prévision de demande", status: "active", onClick: () => go("commandes") },
    { name: "Optimisation slotting", status: "active", onClick: () => go("stockage", "top5-produits-in-out") },
    { name: "Prédiction ETA", status: "active", onClick: () => go("transport", "eta-section") },
    { name: "Détection d'anomalies", status: "active", onClick: () => go("transport", "anom-section") },
  ];

  // Alertes dynamiques (inchangé hormis ce qui existait déjà dans ta version)
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
          <Button size="sm" onClick={() => { refreshTopKpis(); refreshAlerts(); }} disabled={loadingAlerts}>
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

      {/* Placeholder performance */}
      <Card>
        <CardHeader>
          <CardTitle>Performance de la chaîne logistique</CardTitle>
          <CardDescription>Métriques clés sur les dernières 24h</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="h-64 flex items-center justify-center text-muted-foreground">
            <div className="text-center">
              <BarChart3 className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>Graphiques de performance</p>
              <p className="text-sm">Intégration avec Plotly/Chart.js à venir</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default SupervisorDashboard;
export { SupervisorDashboard };

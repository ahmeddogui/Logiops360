import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
  Warehouse,
  Package,
  TrendingUp,
  MapPin,
  AlertTriangle,
  RefreshCw,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

/* Recharts pour les nouveaux graphes */
import {
  ResponsiveContainer,
  ComposedChart,
  BarChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  LabelList, // 
} from "recharts";

const API = (import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000/api").replace(/\/$/, "");

/** ====== Types d’API ====== */
type StorageKpis = {
  occupancy_rate: number;
  active_locations: number;
  distinct_skus: number;
  saturated_locations_pct: number;
  totals: { on_hand: number; capacity: number };
};

type ZonesItem = {
  zone: string;
  n_locations: number;
  on_hand: number;
  capacity: number;
  occupancy_pct: number;
  status: "ok" | "alerte" | "critique";
};
type ZonesResponse = { items: ZonesItem[] };

type HotspotItem = {
  location?: string | null;
  referenceproduit?: string | null;
  capacity_pct?: number | null;
  reason: string;
  action?: string | null;
};
type HotspotsResponse = { items: HotspotItem[] };

type SlottingResponse = {
  summary: { fast_capacity: number; fast_used_before: number; fast_used_after: number; moves: number };
  sample: Array<{ referenceproduit: string; from_zone: string; to_zone: string; move_qty: number; reason: string }>;
  csv_path: string;
};

type MapItem = {
  support_label: string | null;
  x: number | null;
  y: number | null;
  z: number | null;
  norm: number | null;
  lat: number | null;
  lon: number | null;
  n_locations: number;
  on_hand: number;
  capacity: number;
  occupancy_pct: number;
};
type MapResponse = { items: MapItem[] };

type LocationDetail = {
  location: string;
  on_hand: number;
  n_skus: number;
  capacity: number;
  occ_ratio: number;
  products: { reference: string; qty: number }[];
};

/* Types Analytics (nouveaux) */
type ClassDistributionItem = { class: string; nb_products: number; total_qty: number };
type TopStoragePointItem = { label: string; total_volume: number; x_coord: number | null; y_coord: number | null; z_coord: number | null; norm: number | null };
type StorageAnalyticsResponse = { class_distribution: ClassDistributionItem[]; top_storage_points: TopStoragePointItem[] };

const fmtInt = (n?: number) => (typeof n === "number" ? n.toLocaleString("fr-FR") : "—");
const fmtPct = (x?: number) => typeof x === "number" && isFinite(x) ? `${Math.round(x * 100).toLocaleString("fr-FR")}%` : "—";

/* ⬇️ Helpers pour l’affichage des grands nombres sur les axes */
const fmtCompact = (n: number) =>
  new Intl.NumberFormat("fr-FR", { notation: "compact", maximumFractionDigits: 1 }).format(n);
const fmtFull = (n: number) => new Intl.NumberFormat("fr-FR").format(n);

const zoneMeta = (z: string) => {
  const code = (z || "?").toUpperCase();
  if (code === "A") return { nom: "Zone A - Fast Moving", type: "haute_rotation" as const };
  if (code === "B") return { nom: "Zone B - Medium", type: "moyenne_rotation" as const };
  if (code === "C") return { nom: "Zone C - Slow Moving", type: "basse_rotation" as const };
  if (code === "D") return { nom: "Zone D - Réserve", type: "reserve" as const };
  return { nom: `Zone ${code}`, type: "reserve" as const };
};

export const StockageDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [stockageStats, setStockageStats] = useState<any[]>([]);
  const [zones, setZones] = useState<any[]>([]);
  const [optimisations, setOptimisations] = useState<any[]>([]);
  const [emplacementsCritiques, setEmplacementsCritiques] = useState<any[]>([]);
  const [mapItems, setMapItems] = useState<MapItem[]>([]);
  const [selectedLoc, setSelectedLoc] = useState<string | null>(null);
  const [locDetails, setLocDetails] = useState<LocationDetail | null>(null);
  const [slotting, setSlotting] = useState<SlottingResponse | null>(null);
  const [analytics, setAnalytics] = useState<StorageAnalyticsResponse | null>(null);

  const fetchKpis = async (): Promise<StorageKpis> => {
    const r = await fetch(`${API}/storage/kpis?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /storage/kpis ${r.status}`);
    return j as StorageKpis;
  };
  const fetchZones = async (): Promise<ZonesResponse> => {
    const r = await fetch(`${API}/storage/zones/occupancy?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /storage/zones/occupancy ${r.status}`);
    return j as ZonesResponse;
  };
  const fetchHotspots = async (): Promise<HotspotsResponse> => {
    const r = await fetch(`${API}/storage/hotspots?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /storage/hotspots ${r.status}`);
    return j as HotspotsResponse;
  };
  const fetchSlotting = async (): Promise<SlottingResponse> => {
    const r = await fetch(`${API}/storage/slotting/plan?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /storage/slotting/plan ${r.status}`);
    return j as SlottingResponse;
  };
  const fetchMap = async (): Promise<MapResponse> => {
    const r = await fetch(`${API}/storage/map?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /storage/map ${r.status}`);
    return j as MapResponse;
  };
  const fetchLocDetail = async (loc: string) => {
    const r = await fetch(`${API}/storage/location/${loc}?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (r.ok) setLocDetails(j as LocationDetail);
  };
  const fetchStorageAnalytics = async (): Promise<StorageAnalyticsResponse> => {
    const r = await fetch(`${API}/storage/analytics?t=${Date.now()}`, { cache: "no-store" as RequestCache });
    const j = await r.json();
    if (!r.ok) throw new Error(j?.error || `GET /storage/analytics ${r.status}`);
    return j as StorageAnalyticsResponse;
  };

  const handleOptimize = async () => {
    try {
      setLoading(true);
      const [slot, hs] = await Promise.all([fetchSlotting(), fetchHotspots()]);
      setSlotting(slot);
      setOptimisations([
        { type: "Slotting", description: `Repositionner ${slot.summary.moves} produits`, priorite: (slot.summary.moves || 0) > 0 ? "haute" : "moyenne" } as const,
      ]);
    } catch (e: any) {
      setError(e?.message || "Erreur lors de l’optimisation");
    } finally {
      setLoading(false);
    }
  };

  const refreshAll = async () => {
    setLoading(true);
    setError(null);
    try {
      const [k, z, hs, slot, map, an] = await Promise.all([
        fetchKpis(),
        fetchZones(),
        fetchHotspots(),
        fetchSlotting(),
        fetchMap(),
        fetchStorageAnalytics(),
      ]);

      setStockageStats([
        { title: "Taux d'occupation", value: fmtPct(k.occupancy_rate), change: undefined, icon: Warehouse, color: "text-success" },
        { title: "Emplacements actifs", value: fmtInt(k.active_locations), change: undefined, icon: MapPin, color: "text-primary" },
        { title: "Produits différents", value: fmtInt(k.distinct_skus), change: undefined, icon: Package, color: "text-success" },
        { title: "Emplacements saturés", value: fmtPct(k.saturated_locations_pct), change: undefined, icon: RefreshCw, color: "text-warning" },
      ]);

      setZones((z.items || []).map((it) => {
        const meta = zoneMeta(it.zone);
        return {
          nom: meta.nom,
          occupation: Math.round((it.occupancy_pct || 0) * 100),
          emplacements: it.n_locations || 0,
          type: meta.type,
          alert: it.status === "critique",
        };
      }));

      setEmplacementsCritiques((hs.items || []).slice(0, 12).map((h) => ({
        emplacement: h.location ?? "—",
        produit: h.referenceproduit ?? "—",
        probleme: h.reason ?? "—",
        capacite: typeof h.capacity_pct === "number" ? Math.max(0, Math.min(100, Math.round(h.capacity_pct))) : 0,
      })));

      setOptimisations([
        { type: "Slotting", description: `Repositionner ${slot.summary.moves} produits`, priorite: (slot.summary.moves || 0) > 0 ? "haute" : "moyenne" } as const,
      ]);

      setSlotting(slot);
      setMapItems(map.items || []);
      setAnalytics(an);
    } catch (e: any) {
      setError(e?.message || "Erreur lors du chargement");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { refreshAll(); }, []);

  // ====== Top 5 IN/OUT ======
  const top5IN = useMemo(() => {
    if (!slotting || !Array.isArray(slotting.sample)) return [];
    const agg: Record<string, number> = {};
    for (const s of slotting.sample) {
      if ((s.to_zone || "").toUpperCase() === "A") {
        const key = s.referenceproduit || "?";
        agg[key] = (agg[key] || 0) + (s.move_qty || 0);
      }
    }
    return Object.entries(agg).map(([ref, qty]) => ({ ref, qty })).sort((a, b) => b.qty - a.qty).slice(0, 5);
  }, [slotting]);

  const top5OUT = useMemo(() => {
    if (!slotting || !Array.isArray(slotting.sample)) return [];
    const agg: Record<string, number> = {};
    for (const s of slotting.sample) {
      if ((s.from_zone || "").toUpperCase() === "A") {
        const key = s.referenceproduit || "?";
        agg[key] = (agg[key] || 0) + (s.move_qty || 0);
      }
    }
    return Object.entries(agg).map(([ref, qty]) => ({ ref, qty })).sort((a, b) => b.qty - a.qty).slice(0, 5);
  }, [slotting]);

  return (
    <div className="space-y-6 p-6">
      {/* En-tête */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-primary">Gestion du Stockage</h1>
        </div>
        <Button size="sm" onClick={handleOptimize} disabled={loading}>Optimiser slotting</Button>
      </div>

      {/* Statistiques principales */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stockageStats.map((stat, i) => (
          <Card key={i}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">{stat.title}</CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent><div className="text-2xl font-bold text-primary">{stat.value}</div></CardContent>
          </Card>
        ))}
      </div>

      {/* Occupation par zones + Optimisations */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Occupation zones */}
        <Card>
          <CardHeader>
            <CardTitle>Occupation par zones</CardTitle>
            <CardDescription>État en temps réel de l'entrepôt</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {zones.map((zone, i) => (
              <div key={i}>
                <div className="flex justify-between">
                  <span>{zone.nom}</span>
                  <span className="text-sm">{zone.emplacements} empl.</span>
                </div>
                <Progress value={zone.occupation} />
              </div>
            ))}
          </CardContent>
        </Card>

        {/* Optimisations IA + nouveaux graphes */}
        <Card>
          <CardHeader>
            <CardTitle>Optimisations IA</CardTitle>
            <CardDescription>Slotting, IN/OUT & analytics</CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            {/* Top 5 IN/OUT */}
            <div>
              <h4 className="font-semibold">Top 5 Produits IN / OUT</h4>
              <div className="grid grid-cols-2 gap-6">
                {/* IN */}
                <div>
                  <h5 className="text-green-600 font-medium">IN</h5>
                  <ul>
                    {top5IN.map((p, i) => (
                      <li key={i} className="flex justify-between">
                        <span>{p.ref}</span>
                        <span className="font-medium">{p.qty}</span>
                      </li>
                    ))}
                    {/* Données fixes ajoutées sous le IN */}
                    {[
                      { ref: "8BSFSA", qty: 115 },
                      { ref: "BLJLMQ", qty: 123 },
                      { ref: "U8203G", qty: 100 },
                      { ref: "ZX6MUV", qty: 112 },
                      { ref: "0SJH1Z", qty: 188 },
                    ].map((p, i) => (
                      <li key={`fixed-${i}`} className="flex justify-between text-muted-foreground">
                        <span>{p.ref}</span>
                        <span className="font-medium">{p.qty}</span>
                      </li>
                    ))}
                  </ul>
                </div>

                {/* OUT */}
                <div>
                  <h5 className="text-red-600 font-medium">OUT</h5>
                  <ul>
                    {top5OUT.length > 0 ? (
                      top5OUT.map((p, i) => (
                        <li key={i} className="flex justify-between">
                          <span>{p.ref}</span>
                          <span className="font-medium">{p.qty}</span>
                        </li>
                      ))
                    ) : (
                      <p className="text-sm text-muted-foreground">Aucun mouvement OUT</p>
                    )}
                  </ul>
                </div>
              </div>
            </div>

            {/* Graphe 1 : Répartition produits par classe */}
            <div className="mt-32 h-64">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart
                  data={analytics?.class_distribution || []}
                  margin={{ top: 16, right: 12, bottom: 8, left: 12 }}
                >
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="class" />
                  <YAxis
                    yAxisId="left"
                    width={70}
                    tickFormatter={(v) => fmtCompact(Number(v))}
                    allowDecimals={false}
                  />
                  <YAxis
                    yAxisId="right"
                    orientation="right"
                    width={70}
                    tickFormatter={(v) => fmtCompact(Number(v))}
                    allowDecimals={false}
                  />
                  <Tooltip
                    formatter={(val: any, name) =>
                      [fmtFull(Number(val)), name === "nb_products" ? "Nb produits" : "Qté totale"]
                    }
                  />
                  <Bar yAxisId="left" dataKey="nb_products" fill="hsl(var(--primary))">
                    <LabelList
                      dataKey="nb_products"
                      position="top"
                      formatter={(v: any) => fmtCompact(Number(v))}
                      className="text-[10px]"
                    />
                  </Bar>
                  <Line
                    yAxisId="right"
                    type="monotone"
                    dataKey="total_qty"
                    stroke="hsl(var(--secondary))"
                    strokeWidth={2}
                    dot={false}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>

            {/* Graphe 2 : Top 10 points de stockage */}
            <div className="mt-70 h-72">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart
                  data={(analytics?.top_storage_points || []).filter(d => Number(d.total_volume) > 0)}
                  margin={{ top: 24, right: 12, bottom: 40, left: 12 }}
                  barCategoryGap={12}
                  barGap={4}>

                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="label"
                    interval={0}
                    angle={-30}
                    textAnchor="end"
                    tick={{ fontSize: 11 }}
                    height={50}
                  />
                  <YAxis
                    width={70}
                    tickFormatter={(v) => fmtCompact(Number(v))}
                    allowDecimals={false}
                  />
                  <Tooltip
                    formatter={(val: any) => fmtFull(Number(val))}
                    labelFormatter={(lbl) => `Emplacement: ${lbl}`}
                  />
                  <Bar dataKey="total_volume" fill="hsl(var(--primary))">
                    <LabelList
                      dataKey="total_volume"
                      position="top"
                      formatter={(v: any) => fmtCompact(Number(v))}
                      className="text-[10px]"
                    />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Emplacements critiques */}
      <Card>
        <CardHeader><CardTitle>Emplacements critiques</CardTitle></CardHeader>
        <CardContent>
          {emplacementsCritiques.map((emp, i) => (
            <div key={i} className="flex justify-between border p-2 rounded">
              <span>{emp.emplacement} — {emp.produit}</span>
              <span>{emp.capacite}%</span>
            </div>
          ))}
        </CardContent>
      </Card>

      {error && <div className="text-sm text-red-600">{error}</div>}

      {/* Dialog détails emplacement */}
      <Dialog open={!!selectedLoc} onOpenChange={() => { setSelectedLoc(null); setLocDetails(null); }}>
        <DialogContent>
          <DialogHeader><DialogTitle>Détails emplacement {locDetails?.location}</DialogTitle></DialogHeader>
          {locDetails ? (
            <div>
              <p><b>Quantité totale :</b> {locDetails.on_hand}</p>
              <p><b>Nb références :</b> {locDetails.n_skus}</p>
              <p><b>Capacité :</b> {locDetails.capacity}</p>
              <p><b>Taux d’occupation :</b> {Math.round(locDetails.occ_ratio * 100)}%</p>
            </div>
          ) : <p>Chargement...</p>}
        </DialogContent>
      </Dialog>
    </div>
  );
};

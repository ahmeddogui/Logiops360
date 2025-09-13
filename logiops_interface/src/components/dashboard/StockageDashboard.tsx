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
  Search,
} from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

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

const fmtInt = (n?: number) => (typeof n === "number" ? n.toLocaleString("fr-FR") : "—");
const fmtPct = (x?: number) =>
  typeof x === "number" && isFinite(x) ? `${Math.round(x * 100).toLocaleString("fr-FR")}%` : "—";

const zoneMeta = (z: string) => {
  const code = (z || "?").toUpperCase();
  if (code === "A") return { nom: "Zone A - Fast Moving", type: "haute_rotation" as const };
  if (code === "B") return { nom: "Zone B - Medium", type: "moyenne_rotation" as const };
  if (code === "C") return { nom: "Zone C - Slow Moving", type: "basse_rotation" as const };
  if (code === "D") return { nom: "Zone D - Réserve", type: "reserve" as const };
  return { nom: `Zone ${code}`, type: "reserve" as const };
};

const colorForOccupancy = (p: number) => {
  const t = Math.max(0, Math.min(1, p));
  const from = { r: 16, g: 185, b: 129 };
  const to = { r: 239, g: 68, b: 68 };
  const r = Math.round(from.r + (to.r - from.r) * t);
  const g = Math.round(from.g + (to.g - from.g) * t);
  const b = Math.round(from.b + (to.b - from.b) * t);
  return `rgb(${r},${g},${b})`;
};

export const StockageDashboard = () => {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [stockageStats, setStockageStats] = useState(
    [
      { title: "Taux d'occupation", value: "—", change: undefined, icon: Warehouse, color: "text-success" },
      { title: "Emplacements actifs", value: "—", change: undefined, icon: MapPin, color: "text-primary" },
      { title: "Produits différents", value: "—", change: undefined, icon: Package, color: "text-success" },
      { title: "Emplacements saturés", value: "—", change: undefined, icon: RefreshCw, color: "text-warning" },
    ] as { title: string; value: string; change?: string; icon: any; color: string }[]
  );

  const [zones, setZones] = useState(
    [] as { nom: string; occupation: number; emplacements: number; type: string; alert: boolean }[]
  );

  const [optimisations, setOptimisations] = useState(
    [] as { type: string; description: string; priorite: "critique" | "haute" | "moyenne" }[]
  );

  const [emplacementsCritiques, setEmplacementsCritiques] = useState(
    [] as { emplacement: string; produit: string; probleme: string; capacite: number }[]
  );

  const [mapItems, setMapItems] = useState<MapItem[]>([]);

  const [selectedLoc, setSelectedLoc] = useState<string | null>(null);
  const [locDetails, setLocDetails] = useState<LocationDetail | null>(null);

  // ====== AJOUT : on conserve la réponse slotting pour afficher le Top 5 IN/OUT ======
  const [slotting, setSlotting] = useState<SlottingResponse | null>(null);

  // ====== AJOUT : scroll doux vers la section Top 5 ======
  const scrollToTop5 = () => {
    const el = document.getElementById("top5-produits-in-out");
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  };

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

  const handleOptimize = async () => {
    try {
      setLoading(true);
      const [slot, hs] = await Promise.all([fetchSlotting(), fetchHotspots()]);

      // AJOUT : garder slotting à jour pour le Top 5
      setSlotting(slot);

      const nbRupture = (hs.items || []).filter((i) => (i.reason || "").toLowerCase().includes("rupture")).length;
      const opt = [
        { type: "Slotting", description: `Repositionner ${slot.summary.moves} produits`, priorite: (slot.summary.moves || 0) > 0 ? "haute" : "moyenne" } as const,
        { type: "Réassort", description: `${nbRupture} références en rupture prévue`,  priorite: nbRupture > 0 ? "critique" : "moyenne" } as const,
      ];
      setOptimisations(opt);
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
      const [k, z, hs, slot, map] = await Promise.all([fetchKpis(), fetchZones(), fetchHotspots(), fetchSlotting(), fetchMap()]);

      const stats = [
        { title: "Taux d'occupation", value: fmtPct(k.occupancy_rate), change: undefined, icon: Warehouse, color: "text-success" },
        { title: "Emplacements actifs", value: fmtInt(k.active_locations), change: undefined, icon: MapPin, color: "text-primary" },
        { title: "Produits différents", value: fmtInt(k.distinct_skus), change: undefined, icon: Package, color: "text-success" },
        { title: "Emplacements saturés", value: fmtPct(k.saturated_locations_pct), change: undefined, icon: RefreshCw, color: "text-warning" },
      ];
      setStockageStats(stats);

      const zonesUi = (z.items || []).map((it) => {
        const meta = zoneMeta(it.zone);
        return {
          nom: meta.nom,
          occupation: Math.round((it.occupancy_pct || 0) * 100),
          emplacements: it.n_locations || 0,
          type: meta.type,
          alert: it.status === "critique",
        };
      });
      setZones(zonesUi);

      const emps = (hs.items || []).slice(0, 12).map((h) => ({
        emplacement: h.location ?? "—",
        produit: h.referenceproduit ?? "—",
        probleme: h.reason ?? "—",
        capacite: typeof h.capacity_pct === "number" ? Math.max(0, Math.min(100, Math.round(h.capacity_pct))) : 0,
      }));
      setEmplacementsCritiques(emps);

      const nbRupture = (hs.items || []).filter((i) => (i.reason || "").toLowerCase().includes("rupture")).length;
      const opt = [
        { type: "Slotting", description: `Repositionner ${slot.summary.moves} produits`, priorite: (slot.summary.moves || 0) > 0 ? "haute" : "moyenne" } as const,
        { type: "Réassort", description: `${nbRupture} références en rupture prévue`,  priorite: nbRupture > 0 ? "critique" : "moyenne" } as const,
      ];
      setOptimisations(opt);

      // AJOUT : conserver la réponse slotting
      setSlotting(slot);

      setMapItems(map.items || []);
    } catch (e: any) {
      setError(e?.message || "Erreur lors du chargement");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /** ====== Projection: priorité à x/y ; sinon lat/lon ; sinon grille ====== */
  const svgPoints = useMemo(() => {
    const itemsXY = (mapItems || []).filter((d) => typeof d.x === "number" && typeof d.y === "number");
    const itemsLL = (mapItems || []).filter((d) => typeof d.lat === "number" && typeof d.lon === "number");
    const W = 800;
    const H = 240;
    const pad = 16;

    if (itemsXY.length >= 2) {
      const xs = itemsXY.map((d) => d.x as number);
      const ys = itemsXY.map((d) => d.y as number);
      const minX = Math.min(...xs), maxX = Math.max(...xs);
      const minY = Math.min(...ys), maxY = Math.max(...ys);
      return {
        viewBox: `0 0 ${W} ${H}`,
        width: "100%",
        height: H,
        points: itemsXY.map((d, i) => {
          const x = pad + ((d.x! - minX) / (maxX - minX || 1)) * (W - pad * 2);
          const y = pad + (1 - (d.y! - minY) / (maxY - minY || 1)) * (H - pad * 2);
          const occ = Math.max(0, Math.min(1, d.occupancy_pct || 0));
          const r = 4 + Math.min(12, Math.max(0, d.n_locations / 50));
          return {
            key: `${d.support_label ?? "?"}-xy-${i}`,
            x, y, r,
            fill: colorForOccupancy(occ),
            title: `${d.support_label ?? "Support ?"} • ${Math.round(occ * 100)}% • ${d.n_locations} empl.`,
          };
        }),
      };
    }

    if (itemsLL.length >= 2) {
      const lats = itemsLL.map((d) => d.lat as number);
      const lons = itemsLL.map((d) => d.lon as number);
      const minLat = Math.min(...lats), maxLat = Math.max(...lats);
      const minLon = Math.min(...lons), maxLon = Math.max(...lons);
      return {
        viewBox: `0 0 ${W} ${H}`,
        width: "100%",
        height: H,
        points: itemsLL.map((d, i) => {
          const x = pad + ((d.lon! - minLon) / (maxLon - minLon || 1)) * (W - pad * 2);
          const y = pad + (1 - (d.lat! - minLat) / (maxLat - minLat || 1)) * (H - pad * 2);
          const occ = Math.max(0, Math.min(1, d.occupancy_pct || 0));
          const r = 4 + Math.min(12, Math.max(0, d.n_locations / 50));
          return {
            key: `${d.support_label ?? "?"}-ll-${i}`,
            x, y, r,
            fill: colorForOccupancy(occ),
            title: `${d.support_label ?? "Support ?"} • ${Math.round(occ * 100)}% • ${d.n_locations} empl.`,
          };
        }),
      };
    }

    // Fallback: grille
    const all = mapItems || [];
    const cols = 10;
    const cellW = 72;
    const cellH = 40;
    const rows = Math.ceil(all.length / cols);
    const Wg = pad * 2 + cols * cellW;
    const Hg = pad * 2 + rows * cellH;
    const points = all.map((d, i) => {
      const col = i % cols;
      const row = Math.floor(i / cols);
      const x = pad + col * cellW + cellW / 2;
      const y = pad + row * cellH + cellH / 2;
      const occ = Math.max(0, Math.min(1, d.occupancy_pct || 0));
      const r = 4 + Math.min(12, Math.max(0, d.n_locations / 50));
      return {
        key: `${d.support_label ?? "?"}-grid-${i}`,
        x, y, r,
        fill: colorForOccupancy(occ),
        title: `${d.support_label ?? "Support ?"} • ${Math.round(occ * 100)}% • ${d.n_locations} empl.`,
      };
    });
    return { viewBox: `0 0 ${Wg} ${Hg}`, width: "100%", height: Math.min(280, Hg), points };
  }, [mapItems]);

  // ====== AJOUT : calcul Top 5 IN/OUT à partir de slotting.sample ======
  const top5IN = useMemo(() => {
    if (!slotting || !Array.isArray(slotting.sample)) return [] as { ref: string; qty: number }[];
    const agg: Record<string, number> = {};
    for (const s of slotting.sample) {
      if ((s.to_zone || "").toUpperCase() === "A") {
        const key = s.referenceproduit || "?";
        agg[key] = (agg[key] || 0) + (s.move_qty || 0);
      }
    }
    return Object.entries(agg)
      .map(([ref, qty]) => ({ ref, qty }))
      .sort((a, b) => b.qty - a.qty)
      .slice(0, 5);
  }, [slotting]);

  const top5OUT = useMemo(() => {
    if (!slotting || !Array.isArray(slotting.sample)) return [] as { ref: string; qty: number }[];
    const agg: Record<string, number> = {};
    for (const s of slotting.sample) {
      if ((s.from_zone || "").toUpperCase() === "A") {
        const key = s.referenceproduit || "?";
        agg[key] = (agg[key] || 0) + (s.move_qty || 0);
      }
    }
    return Object.entries(agg)
      .map(([ref, qty]) => ({ ref, qty }))
      .sort((a, b) => b.qty - a.qty)
      .slice(0, 5);
  }, [slotting]);

  return (
    <div className="space-y-6 p-6">
      {/* En-tête */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-primary">Gestion du Stockage</h1>
          <p className="text-muted-foreground">Optimisation de l'entrepôt et slotting intelligent</p>
        </div>
        <div className="flex space-x-2">
          <Button variant="outline" size="sm">
            <Search className="h-4 w-4 mr-2" />
            Chercher produit
          </Button>
          <Button size="sm" onClick={handleOptimize} disabled={loading}>
            Optimiser slotting
          </Button>
        </div>
      </div>

      {/* Statistiques principales */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {stockageStats.map((stat, index) => (
          <Card key={index} className="transition-all duration-300 hover:shadow-lg">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">{stat.title}</CardTitle>
              <stat.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-primary">{stat.value}</div>
              {stat.change ? (
                <div className={`text-xs flex items-center mt-1 ${stat.color}`}>
                  <TrendingUp className="h-3 w-3 mr-1" />
                  {stat.change} vs hier
                </div>
              ) : (
                <div className="text-xs mt-1 text-muted-foreground">—</div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Occupation par zones */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <MapPin className="h-5 w-5" />
              <span>Occupation par zones</span>
            </CardTitle>
            <CardDescription>État en temps réel de l'entrepôt</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {zones.map((zone, index) => (
              <div key={index} className="space-y-2">
                <div className="flex justify-between items-center">
                  <div className="flex items-center space-x-2">
                    <span className="font-medium">{zone.nom}</span>
                    {zone.alert && (
                      <Badge variant="destructive">
                        <AlertTriangle className="h-3 w-3 mr-1" />
                        Critique
                      </Badge>
                    )}
                  </div>
                  <span className="text-sm text-muted-foreground">{zone.emplacements} empl.</span>
                </div>
                <div className="flex items-center space-x-2">
                  <Progress value={zone.occupation} className="flex-1 h-2" />
                  <span className="text-sm font-medium w-12">{zone.occupation}%</span>
                </div>
              </div>
            ))}
            {zones.length === 0 && <div className="text-sm text-muted-foreground">Aucune zone</div>}
          </CardContent>
        </Card>

        {/* Optimisations recommandées */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <TrendingUp className="h-5 w-5" />
              <span>Optimisations IA</span>
            </CardTitle>
            <CardDescription>Recommandations</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {optimisations.map((opt, index) => (
              <div key={index} className="p-3 border rounded-lg">
                <div className="flex justify-between items-start mb-2">
                  <div>
                    <div className="font-medium">{opt.type}</div>
                    <div className="text-sm text-muted-foreground">{opt.description}</div>
                  </div>
                  <Badge variant={opt.priorite === "critique" ? "destructive" : opt.priorite === "haute" ? "default" : "secondary"}>
                    {opt.priorite}
                  </Badge>
                </div>
                <div className="flex justify-between items-center">
                  {/* MODIF : sur "Slotting", scroller vers Top 5 ; sinon, on laisse le bouton basique */}
                  {opt.type === "Slotting" ? (
                    <Button size="sm" variant="outline" onClick={scrollToTop5}>
                      Voir
                    </Button>
                  ) : (
                    <Button size="sm" variant="outline" onClick={handleOptimize} disabled={loading}>
                      Voir
                    </Button>
                  )}
                </div>
              </div>
            ))}
            {optimisations.length === 0 && <div className="text-sm text-muted-foreground">Aucune optimisation</div>}
          </CardContent>
        </Card>
      </div>

      {/* Emplacements critiques */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center space-x-2">
            <AlertTriangle className="h-5 w-5" />
            <span>Emplacements nécessitant attention</span>
          </CardTitle>
          <CardDescription>Analyse unified_storage_view + clean_storage_location</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {emplacementsCritiques.map((emp, index) => (
              <div key={index} className="flex items-center justify-between p-3 border rounded-lg">
                <div className="flex items-center space-x-4">
                  <div className="font-mono text-sm bg-muted px-2 py-1 rounded">{emp.emplacement}</div>
                  <div>
                    <div className="font-medium">{emp.produit}</div>
                    <div className="text-sm text-muted-foreground">{emp.probleme}</div>
                  </div>
                </div>
                <div className="flex items-center space-x-3">
                  <div className="text-right">
                    <div className="text-sm text-muted-foreground">Capacité</div>
                    <div className="font-medium">{emp.capacite}%</div>
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => {
                      setSelectedLoc(emp.emplacement);
                      fetchLocDetail(emp.emplacement);
                    }}
                  >
                    Voir
                  </Button>
                </div>
              </div>
            ))}
            {emplacementsCritiques.length === 0 && <div className="text-sm text-muted-foreground">Aucun emplacement critique</div>}
          </div>
        </CardContent>
      </Card>

      {/* ====== Top 5 IN/OUT (CIBLE DU SCROLL) ====== */}
      <Card id="top5-produits-in-out">
        <CardHeader>
          <CardTitle>Top 5 Produits IN / OUT</CardTitle>
          <CardDescription>Flux des mouvements vers et depuis la fast zone (A)</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 className="font-semibold mb-2 text-green-600">Top 5 IN</h4>
              {top5IN.length > 0 ? (
                <ul className="space-y-1">
                  {top5IN.map((p, i) => (
                    <li key={i} className="flex justify-between">
                      <span>{p.ref}</span>
                      <span className="font-medium">{p.qty}</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-muted-foreground">Aucun mouvement IN</p>
              )}
            </div>
            <div>
              <h4 className="font-semibold mb-2 text-red-600">Top 5 OUT</h4>
              {top5OUT.length > 0 ? (
                <ul className="space-y-1">
                  {top5OUT.map((p, i) => (
                    <li key={i} className="flex justify-between">
                      <span>{p.ref}</span>
                      <span className="font-medium">{p.qty}</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-muted-foreground">Aucun mouvement OUT</p>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Schéma de l'entrepôt */}
      <Card>
        <CardHeader>
          <CardTitle>Plan de l'entrepôt</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64 flex items-center justify-center text-muted-foreground border-2 border-dashed rounded-lg">
            {svgPoints.points && svgPoints.points.length > 0 ? (
              <svg viewBox={svgPoints.viewBox} width={svgPoints.width} height={svgPoints.height}>
                <rect x={0} y={0} width="100%" height="100%" fill="white" />
                {svgPoints.points.map((p) => (
                  <g key={p.key}>
                    <circle cx={p.x} cy={p.y} r={p.r} fill={p.fill} opacity={0.9}>
                      <title>{p.title}</title>
                    </circle>
                  </g>
                ))}
              </svg>
            ) : (
              <div className="text-center">
                <Warehouse className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>Schéma interactif de l'entrepôt</p>
                <p className="text-sm">Visualisation SVG/Canvas avec données clean_support_points (x/y)</p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {error && <div className="text-sm text-red-600">{error}</div>}

      {/* Dialog détails emplacement */}
      <Dialog open={!!selectedLoc} onOpenChange={() => { setSelectedLoc(null); setLocDetails(null); }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Détails emplacement {locDetails?.location}</DialogTitle>
          </DialogHeader>
          {locDetails ? (
            <div className="space-y-2">
              <p><b>Quantité totale :</b> {locDetails.on_hand}</p>
              <p><b>Nb références :</b> {locDetails.n_skus}</p>
              <p><b>Capacité :</b> {locDetails.capacity}</p>
              <p><b>Taux d’occupation :</b> {Math.round(locDetails.occ_ratio*100)}%</p>
              <h4 className="mt-2 font-medium">Produits :</h4>
              <ul className="list-disc ml-5">
                {locDetails.products.map((p,i) =>
                  <li key={i}>{p.reference} — {p.qty}</li>
                )}
              </ul>
            </div>
          ) : (
            <p>Chargement...</p>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
};

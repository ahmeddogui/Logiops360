// src/components/ml-cards/DelayRiskCard.tsx
import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { delayList, delayDetail } from "@/services/ml";
import { Clock, MapPin, Truck, Filter } from "lucide-react";

type Props = { token: string };

type Risk = "retard_critique" | "retard" | "limite" | "en_temps" | "unknown";
type Item = {
  shipment_id: string;
  origin: string;
  destination_zone: string;
  carrier: string;
  service_level: string;
  distance_km: number;
  weight_kg: number;
  eta_pred_h: number;
  sla_hours: number | null;
  delta_h: number | null;
  risk: Risk;
  ship_dt?: string | null;
};

const RISK_LABEL: Record<Risk | "all", string> = {
  all: "Tous",
  en_temps: "En temps",
  limite: "À la limite",
  retard: "Retard",
  retard_critique: "Retard critique",
  unknown: "Inconnu",
};

// conversion robuste
function num(v: any): number | null {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(",", ".").trim();
  if (s === "" || s.toLowerCase() === "null" || s.toLowerCase() === "nan") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

export default function DelayRiskCard({ token }: Props) {
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [limit, setLimit] = useState(40);

  const [activeRisk, setActiveRisk] = useState<Risk | "all">("all");

  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<any | null>(null);

  useEffect(() => {
    async function load() {
      setLoading(true); setError(null);
      try {
        const r = await delayList(token, limit);
        setItems(r.items || []);
      } catch (e: any) {
        setError(e.message ?? "Erreur");
      } finally {
        setLoading(false);
      }
    }
    if (token) load();
  }, [token, limit]);

  const counts = useMemo(() => {
    const c: Record<Risk, number> = {
      en_temps: 0, limite: 0, retard: 0, retard_critique: 0, unknown: 0
    };
    for (const it of items) c[it.risk] = (c[it.risk] ?? 0) + 1;
    return c;
  }, [items]);

  const filtered = useMemo(() => {
    if (activeRisk === "all") return items;
    return items.filter((it) => it.risk === activeRisk);
  }, [items, activeRisk]);

  // Event global pour TopKpis
  useEffect(() => {
    const total = filtered.length;
    const late = filtered.filter(
      (x) => x.risk === "retard" || x.risk === "retard_critique"
    ).length;

    window.dispatchEvent(
      new CustomEvent("delay:list-updated", { detail: { total, late } })
    );
  }, [filtered]);

  function riskBadge(r: Item["risk"]) {
    const base = "whitespace-nowrap rounded-full px-2.5 py-0.5 text-[12px]";
    switch (r) {
      case "en_temps":        return <Badge className={base}>En temps</Badge>;
      case "limite":          return <Badge variant="secondary" className={base}>À la limite</Badge>;
      case "retard":          return <Badge variant="destructive" className={base}>Retard</Badge>;
      case "retard_critique": return <Badge variant="destructive" className={base}>Retard critique</Badge>;
      default:                return <Badge variant="outline" className={base}>Inconnu</Badge>;
    }
  }

  async function openDetail(id: string) {
    try {
      const d = await delayDetail(token, id);
      setDetail(d);
      setOpen(true);
    } catch (e: any) {
      setError(e.message ?? "Erreur");
    }
  }

  function FilterBtn({ value, count }: { value: Risk | "all"; count?: number; }) {
    const selected = activeRisk === value;
    return (
      <Button
        variant={selected ? "default" : "outline"}
        size="sm"
        onClick={() => setActiveRisk(value)}
      >
        {value === "all" ? RISK_LABEL.all : RISK_LABEL[value]}
        {typeof count === "number" && (
          <Badge variant={selected ? "secondary" : "outline"} className="ml-2">{count}</Badge>
        )}
      </Button>
    );
  }

  return (
    <Card className="transition-all hover:shadow-md">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Clock className="h-5 w-5" /> Risque de retard sur les livraisons
        </CardTitle>
        <CardDescription>Prédiction vs SLA • filtres • détail</CardDescription>
      </CardHeader>

      <CardContent className="space-y-4">
        {loading && <div className="text-sm text-muted-foreground">Chargement…</div>}
        {error && <div className="text-sm text-destructive">{error}</div>}

        {/* barre d’actions */}
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setLoading(true); setError(null);
              delayList(token, limit)
                .then(r => setItems(r.items || []))
                .catch(e => setError(e.message ?? "Erreur"))
                .finally(() => setLoading(false));
            }}
          >
            Rafraîchir
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setLimit(n => n + 20)}
            disabled={loading}
          >
            Voir +20
          </Button>

          <div className="h-6 w-px bg-border mx-1" />

          {/* Filtres statut */}
          <div className="flex flex-wrap gap-2">
            <FilterBtn value="all" count={items.length} />
            <FilterBtn value="en_temps" count={counts.en_temps} />
            <FilterBtn value="limite" count={counts.limite} />
            <FilterBtn value="retard" count={counts.retard} />
            <FilterBtn value="retard_critique" count={counts.retard_critique} />
            <FilterBtn value="unknown" count={counts.unknown} />
          </div>

          <div className="ml-auto text-xs text-muted-foreground flex items-center gap-1">
            <Filter className="h-3.5 w-3.5" /> Affichés: {filtered.length}/{items.length}
          </div>
        </div>

        {/* LISTE SCROLLABLE */}
        <div className="max-h-[560px] overflow-y-auto pr-1">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {filtered.map((it) => {
              // Retard estimé sur la tuile (delta_h prioritaire)
              const eta = num(it.eta_pred_h);
              const sla = num(it.sla_hours);
              const delta = num(it.delta_h);
              const delay_h = delta ?? (eta !== null && sla !== null ? eta - sla : null);
              const delayText = delay_h === null ? "—" : `${Math.max(0, delay_h).toFixed(2)} h`;

              return (
                <div
                  key={it.shipment_id}
                  className="p-4 border rounded-xl hover:bg-accent/40 transition flex items-stretch justify-between min-h-[96px]"
                >
                  <div
                    className="flex-1 cursor-pointer"
                    onClick={() => openDetail(it.shipment_id)}
                  >
                    <div className="flex items-start gap-3">
                      <Truck className="h-4 w-4 mt-1 text-muted-foreground" />
                      <div className="space-y-1">
                        <div className="flex items-center gap-2">
                          <span className="font-mono text-[11px] text-muted-foreground">{it.shipment_id}</span>
                          {riskBadge(it.risk)}
                        </div>

                        <div className="text-sm font-medium">
                          {it.origin} <span className="mx-1 text-muted-foreground">⇒</span> {it.destination_zone}
                        </div>

                        <div className="text-xs text-muted-foreground">{it.carrier}</div>

                        <div className="text-xs text-muted-foreground">
                          {Math.round(it.distance_km)} km • {Math.round(it.weight_kg)} kg
                        </div>

                        {/* ⬇️ Remplacé par Retard estimé */}
                        <div className="text-xs">
                          Retard estimé ≈ <span className="font-semibold">{delayText}</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="flex flex-col items-end justify-between ml-3">
                    <div className="text-right text-[11px] text-muted-foreground whitespace-nowrap">
                      {it.ship_dt?.replace("T"," ").slice(0,16)}
                    </div>
                    <Button size="sm" variant="outline" onClick={() => openDetail(it.shipment_id)}>
                      Détail
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>

          {!loading && filtered.length === 0 && (
            <div className="text-sm text-muted-foreground mt-3">Aucune livraison pour ce filtre.</div>
          )}
        </div>
      </CardContent>

      {/* Popup détail */}
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>Détail livraison</DialogTitle>
            <DialogDescription>Infos complètes + retard estimé</DialogDescription>
          </DialogHeader>

          {detail ? (
            <div className="space-y-2 text-sm">
              {(() => {
                const eta = num(detail?.eta_pred_h);
                const sla = num(detail?.sla_hours ?? detail?.sla_h ?? detail?.sla);
                const delta = num(detail?.delta_h);
                const delay_h = delta ?? (eta !== null && sla !== null ? eta - sla : null);
                const delayDisplay =
                  delay_h === null ? "—" : `${Math.max(0, delay_h).toFixed(2)} h`;

                return (
                  <>
                    <div className="flex items-center gap-2">
                      <MapPin className="h-4 w-4" />
                      <span className="font-medium">{detail.origin}</span>
                      <span className="mx-2 text-muted-foreground">⇒</span>
                      <span className="font-medium">{detail.destination_zone}</span>
                      <span className="ml-2">{riskBadge((detail.risk as Risk) ?? "unknown")}</span>
                      {delay_h !== null && delay_h > 0 && (
                        <Badge variant="destructive" className="ml-2">Retard estimé: {delayDisplay}</Badge>
                      )}
                      {delay_h !== null && delay_h <= 0 && (
                        <Badge className="ml-2">Dans le SLA</Badge>
                      )}
                    </div>

                    {/* Grille sans SLA ni ETA désormais */}
                    <div className="grid grid-cols-2 gap-2">
                      <div><span className="text-muted-foreground">Shipment ID:</span> {detail.shipment_id}</div>
                      <div><span className="text-muted-foreground">Date expédition:</span> {String(detail.ship_dt || detail.ship_datetime || "").replace("T"," ").slice(0,16)}</div>
                      <div><span className="text-muted-foreground">Carrier:</span> {detail.carrier}</div>
                      <div><span className="text-muted-foreground">Service:</span> {detail.service_level}</div>
                      <div><span className="text-muted-foreground">Distance:</span> {detail.distance_km} km</div>
                      <div><span className="text-muted-foreground">Poids:</span> {detail.weight_kg} kg</div>
                      <div className="col-span-2">
                        <span className="text-muted-foreground">Retard estimé:</span>{" "}
                        <span className="font-semibold">{delayDisplay}</span>
                      </div>
                    </div>
                  </>
                );
              })()}
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">Chargement…</div>
          )}

          <DialogFooter className="gap-2">
            <Button
              onClick={() => {
                console.log("Contacter le transporteur:", detail?.carrier, detail?.shipment_id);
              }}
            >
              Contacter le transporteur
            </Button>
            <Button variant="secondary" onClick={() => setOpen(false)}>Fermer</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}

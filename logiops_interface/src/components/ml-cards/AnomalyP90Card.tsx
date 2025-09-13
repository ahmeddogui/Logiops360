// src/components/ml-cards/AnomalyP90Card.tsx
import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Filter, AlertTriangle } from "lucide-react";
import { anomP90List, anomP90Detail } from "@/services/ml";

type Props = { token: string };

type Item = {
  shipment_id: string;
  event_id: number;
  phase: string;
  carrier: string;
  origin: string;
  destination_zone: string;
  distance_km: number | string | null;
  weight_kg: number | string | null;
  duration_h: number | string | null;
  avg_duration_h: number | string | null;
  p50_duration_h: number | string | null;
  p90_duration_h: number | string | null;
  ratio_p90: number | string | null;
  severity: "haute" | "moyenne" | "basse" | "inconnu";
};

function num(v: any): number | null {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(",", ".").trim();
  if (s === "" || s.toLowerCase() === "null" || s.toLowerCase() === "nan") return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

export default function AnomalyP90Card({ token }: Props) {
  const [items, setItems] = useState<Item[]>([]);
  const [limit, setLimit] = useState(30);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<any | null>(null);

  // filtres
  const [sev, setSev] = useState<"all" | "haute" | "moyenne" | "basse">("all");
  const [anomCount, setAnomCount] = useState<"all" | 1 | 2 | 3 | 4 | "5+">("all");

  async function loadList(curLimit = limit) {
    if (!token) return;
    setLoading(true); setError(null);
    try {
      const r = await anomP90List(token, curLimit);
      setItems(r.items || []);
    } catch (e: any) {
      setError(e.message ?? "Erreur");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadList();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token, limit]);

  // Compteurs par sévérité (sur événements)
  const counts = useMemo(() => {
    const c = { haute: 0, moyenne: 0, basse: 0 };
    items.forEach((it) => {
      if (it.severity === "haute") c.haute++;
      else if (it.severity === "moyenne") c.moyenne++;
      else if (it.severity === "basse") c.basse++;
    });
    return c;
  }, [items]);

  // Événements filtrés par sévérité
  const filteredEvents = useMemo(() => {
    if (sev === "all") return items;
    return items.filter((it) => it.severity === sev);
  }, [items, sev]);

  // Regrouper par shipment : 1 tuile = 1 shipment
  type Group = {
    shipment_id: string;
    anomalyCount: number;
    firstEventId: number;
    maxRetardH: number;
    sample: Item;
  };

  const groupedByShipment: Group[] = useMemo(() => {
    const map = new Map<string, Group>();
    for (const it of filteredEvents) {
      const d = num(it.duration_h) ?? 0;
      const p90 = num(it.p90_duration_h) ?? 0;
      const retard = Math.max(0, d - p90);

      const key = it.shipment_id;
      if (!map.has(key)) {
        map.set(key, {
          shipment_id: key,
          anomalyCount: 1,
          firstEventId: it.event_id,
          maxRetardH: retard,
          sample: it,
        });
      } else {
        const g = map.get(key)!;
        g.anomalyCount += 1;
        g.maxRetardH = Math.max(g.maxRetardH, retard);
      }
    }
    return Array.from(map.values()).sort((a, b) =>
      b.anomalyCount - a.anomalyCount || b.maxRetardH - a.maxRetardH
    );
  }, [filteredEvents]);

  // Filtre par nombre d’anomalies (1,2,3,4,5+)
  const groupedFiltered = useMemo(() => {
    if (anomCount === "all") return groupedByShipment;
    if (anomCount === "5+") return groupedByShipment.filter(g => g.anomalyCount >= 5);
    return groupedByShipment.filter(g => g.anomalyCount === anomCount);
  }, [groupedByShipment, anomCount]);

  // 🔴 Event vers le dashboard pour mini-card "Anomalies détectées"
  useEffect(() => {
    // stats #shipments par classe de quantité d'anomalies
    const buckets = { "1": 0, "2": 0, "3": 0, "4": 0, "5+": 0 };
    groupedByShipment.forEach(g => {
      if (g.anomalyCount >= 5) buckets["5+"]++;
      else buckets[String(g.anomalyCount) as "1"|"2"|"3"|"4"]++;
    });

    window.dispatchEvent(
      new CustomEvent("anom:list-updated", {
        detail: {
          shipments: {
            total: groupedByShipment.length, // sur l’onglet courant (filtre sévérité)
            byCount: buckets
          },
          events: {
            total: filteredEvents.length // nb d'événements anormaux après filtre sévérité
          }
        }
      })
    );
  }, [filteredEvents, groupedByShipment]);

  function SevBadge({ s }: { s: Item["severity"] }) {
    const base = "whitespace-nowrap rounded-full px-2.5 py-0.5 text-[12px]";
    if (s === "haute") return <Badge variant="destructive" className={base}>Haute</Badge>;
    if (s === "moyenne") return <Badge className={base}>Moyenne</Badge>;
    if (s === "basse") return <Badge variant="secondary" className={base}>Basse</Badge>;
    return <Badge variant="outline" className={base}>Inconnu</Badge>;
  }

  async function openDetailGroup(g: Group) {
    try {
      const d = await anomP90Detail(token, g.shipment_id, g.firstEventId);
      setDetail({ ...d, selected_event_id: g.firstEventId });
      setOpen(true);
    } catch (e: any) {
      setError(e.message ?? "Erreur");
    }
  }

  function buildMailto(d: any): string {
    const subj = encodeURIComponent(`Anomalie phases - Shipment ${d?.shipment_id} - ${d?.carrier}`);
    const lines = [
      `Bonjour ${d?.carrier || "Transporteur"},`,
      ``,
      `Nous constatons une ou plusieurs phases au-delà du P90 pour le shipment ${d?.shipment_id}.`,
      `Origine: ${d?.origin}  =>  Destination: ${d?.destination_zone}`,
      ``,
      `Détails (event_id | phase | durée h | P90 h):`,
      ...(Array.isArray(d?.phases) ? d.phases.map((p:any) =>
        `- #${p.event_id} | ${p.phase} | ${Number(p.duration_h ?? 0).toFixed(2)} | ${Number(p.p90_duration_h ?? 0).toFixed(2)}`
      ) : []),
      ``,
      `Merci de votre retour et des actions correctives proposées.`,
      ``,
      `Cordialement,`,
      `LogiOps360`,
    ].join("\n");
    return `mailto:?subject=${subj}&body=${encodeURIComponent(lines)}`;
  }

  return (
    <Card className="transition-all hover:shadow-md">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <AlertTriangle className="h-5 w-5" /> Anomalies sur les livraisons en cours
        </CardTitle>
        <CardDescription>Règle P90</CardDescription>
      </CardHeader>

      <CardContent className="space-y-4">
        {/* actions */}
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => loadList()}>
            Rafraîchir
          </Button>
          <Button
            variant="ghost"
            size="sm"
            disabled={loading}
            onClick={() => setLimit((n) => n + 20)}
          >
            Voir +20
          </Button>

          <div className="h-6 w-px bg-border mx-1" />

          {/* Filtre SEVERITE (sur événements) */}
          <div className="flex gap-2">
            <Button variant={sev === "all" ? "default" : "outline"} size="sm" onClick={() => setSev("all")}>
              Tous <Badge variant={sev === "all" ? "secondary" : "outline"} className="ml-2">{items.length}</Badge>
            </Button>
            <Button variant={sev === "haute" ? "default" : "outline"} size="sm" onClick={() => setSev("haute")}>
              Haute <Badge variant={sev === "haute" ? "secondary" : "outline"} className="ml-2">{counts.haute}</Badge>
            </Button>
            <Button variant={sev === "moyenne" ? "default" : "outline"} size="sm" onClick={() => setSev("moyenne")}>
              Moyenne <Badge variant={sev === "moyenne" ? "secondary" : "outline"} className="ml-2">{counts.moyenne}</Badge>
            </Button>
            <Button variant={sev === "basse" ? "default" : "outline"} size="sm" onClick={() => setSev("basse")}>
              Basse <Badge variant={sev === "basse" ? "secondary" : "outline"} className="ml-2">{counts.basse}</Badge>
            </Button>
          </div>

          <div className="h-6 w-px bg-border mx-1" />

          {/* Filtre # d’anomalies par shipment */}
          <div className="flex gap-2">
            <Button variant={anomCount === "all" ? "default" : "outline"} size="sm" onClick={() => setAnomCount("all")}>
              Nb anomalies: Tous
            </Button>
            <Button variant={anomCount === 1 ? "default" : "outline"} size="sm" onClick={() => setAnomCount(1)}>
              1
            </Button>
            <Button variant={anomCount === 2 ? "default" : "outline"} size="sm" onClick={() => setAnomCount(2)}>
              2
            </Button>
            <Button variant={anomCount === 3 ? "default" : "outline"} size="sm" onClick={() => setAnomCount(3)}>
              3
            </Button>
            <Button variant={anomCount === 4 ? "default" : "outline"} size="sm" onClick={() => setAnomCount(4)}>
              4
            </Button>
            <Button variant={anomCount === "5+" ? "default" : "outline"} size="sm" onClick={() => setAnomCount("5+")}>
              5+
            </Button>
          </div>

          <div className="ml-auto text-xs text-muted-foreground flex items-center gap-1">
            <Filter className="h-3.5 w-3.5" /> Shipments: {groupedFiltered.length}
          </div>
        </div>

        {loading && <div className="text-sm text-muted-foreground">Chargement…</div>}
        {error && <div className="text-sm text-destructive">{error}</div>}

        {/* grille tuiles (groupées + filtrées) */}
        <div className="max-h-[520px] overflow-y-auto pr-1">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {groupedFiltered.map((g) => {
              const it = g.sample;
              const maxRet = g.maxRetardH;
              return (
                <div
                  key={g.shipment_id}
                  className="p-4 border rounded-xl hover:bg-accent/40 transition flex items-stretch justify-between min-h-[96px]"
                >
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <span className="font-medium">
                        Anomalies sur {g.anomalyCount} phase{g.anomalyCount > 1 ? "s" : ""}
                      </span>
                      <SevBadge s={it.severity} />
                    </div>

                    <div className="text-xs text-muted-foreground mt-0.5">
                      {it.origin} <span className="mx-1">⇒</span> {it.destination_zone}
                    </div>

                    <div className="text-sm mt-1">{it.carrier}</div>

                    <div className="text-xs mt-1">
                      Retard max estimé ≈ <span className="font-semibold">{maxRet.toFixed(1)} h</span>
                    </div>
                  </div>

                  <div className="flex flex-col items-end justify-between ml-3">
                    <Button size="sm" variant="outline" onClick={() => openDetailGroup(g)}>
                      Analyser
                    </Button>
                  </div>
                </div>
              );
            })}
          </div>

          {(!loading && groupedFiltered.length === 0) && (
            <div className="text-sm text-muted-foreground mt-2">Aucun shipment pour ce filtre.</div>
          )}
        </div>
      </CardContent>

      {/* modal détail */}
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Détail anomalies (par phases)</DialogTitle>
            <DialogDescription>Phases du shipment : rouge = &gt; P90, vert = OK</DialogDescription>
          </DialogHeader>

          {!detail ? (
            <div className="text-sm text-muted-foreground">Chargement…</div>
          ) : (
            <div className="space-y-3 text-sm">
              <div className="flex flex-wrap items-center gap-2">
                <div><span className="text-muted-foreground">Shipment:</span> <span className="font-mono">{detail.shipment_id}</span></div>
                {typeof detail.selected_event_id === "number" && (
                  <div><span className="text-muted-foreground">Event ciblé:</span> #{detail.selected_event_id}</div>
                )}
                <div><span className="text-muted-foreground">Carrier:</span> {detail.carrier}</div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <div>{detail.origin} <span className="mx-1">⇒</span> {detail.destination_zone}</div>
              </div>

              <div className="border rounded-md">
                <div className="grid grid-cols-6 text-xs font-medium px-3 py-2 bg-muted/50 rounded-t-md">
                  <div>Event</div><div>Phase</div><div className="text-right">Durée (h)</div><div className="text-right">P50</div><div className="text-right">P90</div><div className="text-center">Anomalie</div>
                </div>
                <div className="max-h-[320px] overflow-y-auto">
                  {detail.phases?.map((p: any) => {
                    const dur = num(p?.duration_h);
                    const p90 = num(p?.p90_duration_h);
                    const isAnom = dur !== null && p90 !== null && dur > p90;
                    const isSelected = typeof detail.selected_event_id === "number" && p.event_id === detail.selected_event_id;

                    return (
                      <div
                        key={p.event_id}
                        className={[
                          "grid grid-cols-6 px-3 py-2 text-xs border-t",
                          isAnom ? "bg-red-50 dark:bg-red-900/20" : "bg-green-50 dark:bg-green-900/20",
                          isSelected ? "ring-1 ring-red-500" : "",
                        ].join(" ")}
                      >
                        <div className="font-mono">{p.event_id}</div>
                        <div>{p.phase}</div>
                        <div className="text-right">{dur !== null ? dur.toFixed(2) : "—"}</div>
                        <div className="text-right">{num(p?.p50_duration_h) !== null ? num(p?.p50_duration_h)!.toFixed(2) : "—"}</div>
                        <div className="text-right">{p90 !== null ? p90.toFixed(2) : "—"}</div>
                        <div className="text-center">{isAnom ? "🔴" : "🟢"}</div>
                      </div>
                    );
                  })}
                </div>
              </div>

              <div className="pt-1">
                <a href={buildMailto(detail)}>
                  <Button>Contacter le transporteur</Button>
                </a>
              </div>
            </div>
          )}

          <DialogFooter>
            <Button variant="secondary" onClick={() => setOpen(false)}>Fermer</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  );
}

import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Clock } from "lucide-react";
import { predictETA, predictETAById, getEtaDistincts, getEtaShipmentIds } from "@/services/ml";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

type Props = { token: string };

export default function EtaPredictCard({ token }: Props) {
  const [shipmentId, setShipmentId] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ETA séparées
  const [etaById, setEtaById] = useState<number | null>(null);
  const [etaWhatIf, setEtaWhatIf] = useState<number | null>(null);

  // Toggle What-If
  const [showWhatIf, setShowWhatIf] = useState(false);

  // Listes réelles
  const [distincts, setDistincts] = useState<{ origin?: string[]; destination_zone?: string[]; carrier?: string[]; service_level?: string[]; } | null>(null);
  const [shipmentIds, setShipmentIds] = useState<string[]>([]);

  useEffect(() => {
    async function load() {
      try {
        const [d, ids] = await Promise.all([
          getEtaDistincts(token),
          getEtaShipmentIds(token, 200),
        ]);
        setDistincts(d);
        setShipmentIds(ids?.shipment_ids ?? []);
        if (!shipmentId && ids?.shipment_ids?.length) {
          setShipmentId(ids.shipment_ids[0]); // pré-sélection du plus récent
        }
        setForm((f) => ({
          origin: f.origin ?? d.origin?.[0] ?? "PARIS",
          destination_zone: f.destination_zone ?? d.destination_zone?.[0] ?? "EU-WEST",
          carrier: f.carrier ?? d.carrier?.[0] ?? "DHL",
          service_level: f.service_level ?? d.service_level?.[0] ?? "EXPRESS",
          ship_dow: f.ship_dow,
          ship_hour: f.ship_hour,
          distance_km: f.distance_km,
          weight_kg: f.weight_kg,
          volume_m3: f.volume_m3,
          total_units: f.total_units,
          n_lines: f.n_lines,
        }));
      } catch (e) {
        console.error("Erreur chargement distincts/ids:", e);
      }
    }
    if (token) load();
  }, [token]);

  // State What-If
  const [form, setForm] = useState({
    origin: "PARIS",
    destination_zone: "EU-WEST",
    carrier: "DHL",
    service_level: "EXPRESS",
    ship_dow: 2,
    ship_hour: 10,
    distance_km: 480,
    weight_kg: 120,
    volume_m3: 1.6,
    total_units: 24,
    n_lines: 3,
  });
  function setField<K extends keyof typeof form>(k: K, v: any) {
    setForm((f) => ({ ...f, [k]: v }));
  }

  async function runById() {
    setLoading(true); setError(null); setEtaById(null);
    try {
      const res = await predictETAById(shipmentId.trim(), token);
      setEtaById(typeof res.eta_hours === "number" ? res.eta_hours : null);
    } catch (e: any) {
      setError(e.message ?? "Erreur");
    } finally {
      setLoading(false);
    }
  }

  async function runWhatIf() {
    setLoading(true); setError(null); setEtaWhatIf(null);
    try {
      const payload = {
        ...form,
        ship_dow: Number(form.ship_dow),
        ship_hour: Number(form.ship_hour),
        distance_km: Number(form.distance_km),
        weight_kg: Number(form.weight_kg),
        volume_m3: Number(form.volume_m3),
        total_units: Number(form.total_units),
        n_lines: Number(form.n_lines),
      };
      const res = await predictETA([payload], token);
      setEtaWhatIf(res?.eta_hours?.[0] ?? null);
    } catch (e: any) {
      setError(e.message ?? "Erreur");
    } finally {
      setLoading(false);
    }
  }

  return (
    <Card className="transition-all hover:shadow-md">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Clock className="h-5 w-5" /> ETA Predictor
        </CardTitle>
        <CardDescription>Prédiction ETA (h) via modèle LightGBM</CardDescription>
      </CardHeader>

      <CardContent className="space-y-6">
        {/* --- Par Shipment ID (Select + Input + bouton) --- */}
        <div className="space-y-2">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
            <div className="md:col-span-2">
              <label className="text-xs text-muted-foreground">Shipment ID (liste)</label>
              <Select
                value={shipmentId}
                onValueChange={(v) => setShipmentId(v)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Choisir un shipment" />
                </SelectTrigger>
                <SelectContent>
                  {shipmentIds.map((id) => (
                    <SelectItem key={id} value={id}>{id}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="text-xs text-muted-foreground">Shipment ID (saisie libre)</label>
              <Input
                placeholder="SH123..."
                value={shipmentId}
                onChange={(e) => setShipmentId(e.target.value)}
              />
            </div>
          </div>

          <div className="flex gap-2">
            <Button onClick={runById} disabled={!shipmentId.trim() || loading} variant="secondary">
              {loading ? "..." : "Prédire par ID"}
            </Button>

            {etaById !== null && (
              <div className="text-sm self-center">
                <Badge variant="outline">ETA (par ID)</Badge>{" "}
                <span className="font-semibold">{etaById.toFixed(2)} h</span>
              </div>
            )}
          </div>
        </div>

        {/* --- Bouton pour afficher/masquer le formulaire What-If --- */}
        <div className="space-y-2">
          <Button
            variant={showWhatIf ? "secondary" : "default"}
            onClick={() => setShowWhatIf((s) => !s)}
            disabled={loading}
          >
            {showWhatIf ? "Fermer What-If" : "What-If"}
          </Button>

          {showWhatIf && (
            <div className="space-y-4 p-3 border rounded-lg">
              {/* Catégorielles */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <label className="text-xs text-muted-foreground">Origin</label>
                  {distincts ? (
                    <Select value={form.origin} onValueChange={(v) => setField("origin", v)}>
                      <SelectTrigger><SelectValue placeholder="Choisir une origin" /></SelectTrigger>
                      <SelectContent>
                        {(distincts.origin ?? []).map((o) => <SelectItem key={o} value={o}>{o}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input value={form.origin} onChange={(e) => setField("origin", e.target.value)} />
                  )}
                </div>

                <div>
                  <label className="text-xs text-muted-foreground">Destination zone</label>
                  {distincts ? (
                    <Select value={form.destination_zone} onValueChange={(v) => setField("destination_zone", v)}>
                      <SelectTrigger><SelectValue placeholder="Choisir une zone" /></SelectTrigger>
                      <SelectContent>
                        {(distincts.destination_zone ?? []).map((z) => <SelectItem key={z} value={z}>{z}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input value={form.destination_zone} onChange={(e) => setField("destination_zone", e.target.value)} />
                  )}
                </div>

                <div>
                  <label className="text-xs text-muted-foreground">Carrier</label>
                  {distincts ? (
                    <Select value={form.carrier} onValueChange={(v) => setField("carrier", v)}>
                      <SelectTrigger><SelectValue placeholder="Choisir un transporteur" /></SelectTrigger>
                      <SelectContent>
                        {(distincts.carrier ?? []).map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input value={form.carrier} onChange={(e) => setField("carrier", e.target.value)} />
                  )}
                </div>

                <div>
                  <label className="text-xs text-muted-foreground">Service level</label>
                  {distincts ? (
                    <Select value={form.service_level} onValueChange={(v) => setField("service_level", v)}>
                      <SelectTrigger><SelectValue placeholder="Choisir un niveau" /></SelectTrigger>
                      <SelectContent>
                        {(distincts.service_level ?? []).map((s) => <SelectItem key={s} value={s}>{s}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input value={form.service_level} onChange={(e) => setField("service_level", e.target.value)} />
                  )}
                </div>
              </div>

              {/* Numériques */}
              <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                <div>
                  <label className="text-xs text-muted-foreground">Ship DOW (0-6)</label>
                  <Input type="number" min={0} max={6} value={form.ship_dow}
                        onChange={(e) => setField("ship_dow", Number(e.target.value))}/>
                </div>
                <div>
                  <label className="text-xs text-muted-foreground">Ship hour (0-23)</label>
                  <Input type="number" min={0} max={23} value={form.ship_hour}
                        onChange={(e) => setField("ship_hour", Number(e.target.value))}/>
                </div>
                <div>
                    <label className="text-xs text-muted-foreground">Distance (km)</label>
                    <Input type="number" step="1" value={form.distance_km}
                          onChange={(e) => setField("distance_km", Number(e.target.value))}/>
                </div>
                <div>
                    <label className="text-xs text-muted-foreground">Weight (kg)</label>
                    <Input type="number" step="1" value={form.weight_kg}
                          onChange={(e) => setField("weight_kg", Number(e.target.value))}/>
                </div>
                <div>
                    <label className="text-xs text-muted-foreground">Volume (m³)</label>
                    <Input type="number" step="0.1" value={form.volume_m3}
                          onChange={(e) => setField("volume_m3", Number(e.target.value))}/>
                </div>
                <div>
                    <label className="text-xs text-muted-foreground">Total units</label>
                    <Input type="number" step="1" value={form.total_units}
                          onChange={(e) => setField("total_units", Number(e.target.value))}/>
                </div>
                <div>
                    <label className="text-xs text-muted-foreground">N lines</label>
                    <Input type="number" step="1" value={form.n_lines}
                          onChange={(e) => setField("n_lines", Number(e.target.value))}/>
                </div>
              </div>

              {/* Bouton de prédiction What-If */}
              <div className="flex gap-2 pt-1">
                <Button onClick={runWhatIf} disabled={loading}>
                  {loading ? "..." : "Prédire (What-If)"}
                </Button>
                {etaWhatIf !== null && (
                  <div className="text-sm self-center">
                    <Badge variant="outline">ETA (What-If)</Badge>{" "}
                    <span className="font-semibold">{etaWhatIf.toFixed(2)} h</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {error && <div className="text-sm text-destructive">{error}</div>}
      </CardContent>
    </Card>
  );
}

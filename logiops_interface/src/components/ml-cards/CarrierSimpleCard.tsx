import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Truck } from "lucide-react";
import { recoSimpleDistincts, recoSimpleRecommend } from "@/services/ml";

type Props = { token: string };

export default function CarrierSimpleCard({ token }: Props) {
  const [opts, setOpts] = useState<{origin:string[], destination_zone:string[], service_level:string[]}>({origin:[],destination_zone:[],service_level:[]});
  const [loading, setLoading] = useState(false);
  const [error, setError]   = useState<string | null>(null);
  const [best, setBest]     = useState<any | null>(null);

  const [form, setForm] = useState({
    origin: "",
    destination_zone: "",
    service_level: "",
    distance_km: 500,
    weight_kg: 120,
    volume_m3: 1.2,
    total_units: 10,
    n_lines: 3,
    ship_dow: 2,
    ship_hour: 10,
  });

  function setField<K extends keyof typeof form>(k: K, v: any) {
    setForm((f) => ({ ...f, [k]: v }));
  }

  useEffect(() => {
    async function load() {
      try {
        const d = await recoSimpleDistincts(token);
        setOpts(d);
        setForm((f) => ({
          ...f,
          origin: d.origin?.[0] || "",
          destination_zone: d.destination_zone?.[0] || "",
          service_level: d.service_level?.[0] || "",
        }));
      } catch (e) {
        console.error(e);
      }
    }
    if (token) load();
  }, [token]);

  async function onPredict() {
    setLoading(true); setError(null); setBest(null);
    try {
      const res = await recoSimpleRecommend({...form}, token);
      setBest(res.best || null);
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
          <Truck className="h-5 w-5" /> Recommandation de transporteur 
        </CardTitle>
        <CardDescription>Choisis la lane + paramètres, puis “Prédire”.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">

        {/* LANE */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <div>
            <div className="text-xs text-muted-foreground mb-1">Origin</div>
            <Select value={form.origin} onValueChange={(v) => setField("origin", v)}>
              <SelectTrigger><SelectValue placeholder="Choisir" /></SelectTrigger>
              <SelectContent>{opts.origin.map((o) => <SelectItem key={o} value={o}>{o}</SelectItem>)}</SelectContent>
            </Select>
          </div>
          <div>
            <div className="text-xs text-muted-foreground mb-1">Destination zone</div>
            <Select value={form.destination_zone} onValueChange={(v) => setField("destination_zone", v)}>
              <SelectTrigger><SelectValue placeholder="Choisir" /></SelectTrigger>
              <SelectContent>{opts.destination_zone.map((z) => <SelectItem key={z} value={z}>{z}</SelectItem>)}</SelectContent>
            </Select>
          </div>
          <div>
            <div className="text-xs text-muted-foreground mb-1">Service level</div>
            <Select value={form.service_level} onValueChange={(v) => setField("service_level", v)}>
              <SelectTrigger><SelectValue placeholder="Choisir" /></SelectTrigger>
              <SelectContent>{opts.service_level.map((s) => <SelectItem key={s} value={s}>{s}</SelectItem>)}</SelectContent>
            </Select>
          </div>
        </div>

        {/* NUMERIQUES */}
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          <div><div className="text-xs text-muted-foreground mb-1">Distance (km)</div>
            <Input type="number" value={form.distance_km} onChange={(e) => setField("distance_km", Number(e.target.value))} />
          </div>
          <div><div className="text-xs text-muted-foreground mb-1">Weight (kg)</div>
            <Input type="number" value={form.weight_kg} onChange={(e) => setField("weight_kg", Number(e.target.value))} />
          </div>
          <div><div className="text-xs text-muted-foreground mb-1">Volume (m³)</div>
            <Input type="number" step={0.1} value={form.volume_m3} onChange={(e) => setField("volume_m3", Number(e.target.value))} />
          </div>
          <div><div className="text-xs text-muted-foreground mb-1">Total units</div>
            <Input type="number" value={form.total_units} onChange={(e) => setField("total_units", Number(e.target.value))} />
          </div>
          <div><div className="text-xs text-muted-foreground mb-1">N lines</div>
            <Input type="number" value={form.n_lines} onChange={(e) => setField("n_lines", Number(e.target.value))} />
          </div>
          <div><div className="text-xs text-muted-foreground mb-1">Ship DOW (0–6)</div>
            <Input type="number" min={0} max={6} value={form.ship_dow} onChange={(e) => setField("ship_dow", Number(e.target.value))} />
          </div>
          <div><div className="text-xs text-muted-foreground mb-1">Ship hour (0–23)</div>
            <Input type="number" min={0} max={23} value={form.ship_hour} onChange={(e) => setField("ship_hour", Number(e.target.value))} />
          </div>
        </div>

        <div className="flex gap-2">
          <Button onClick={onPredict} disabled={loading}>
            {loading ? "..." : "Prédire"}
          </Button>
          {best && <Badge variant="outline">Top 1</Badge>}
        </div>

        {best && (
          <div className="p-3 border rounded-lg space-y-1">
            <div className="text-sm">Transporteur recommandé :</div>
            <div className="text-base font-semibold">{best.carrier} / {best.service_level}</div>
            <div className="text-xs text-muted-foreground">
              ETA ≈ {Number(best.eta_pred_h).toFixed(1)} h • Coût ≈ {Number(best.cost_pred).toFixed(0)} • Fiabilité ≈ {((1-Number(best.risk))*100).toFixed(0)}% • Score {Number(best.score).toFixed(3)}
            </div>
          </div>
        )}

        {error && <div className="text-sm text-destructive">{error}</div>}
      </CardContent>
    </Card>
  );
}

// src/pages/TransportDashboard.tsx
import { Button } from "@/components/ui/button";
import { BarChart3 } from "lucide-react";

import EtaPredictCard from "@/components/ml-cards/EtaPredictCard";
import DelayRiskCard from "@/components/ml-cards/DelayRiskCard";      // émet "delay:list-updated"
import CarrierSimpleCard from "@/components/ml-cards/CarrierSimpleCard";
import AnomalyP90Card from "@/components/ml-cards/AnomalyP90Card";    // émet "anom:list-updated"
import TopKpis from "@/components/ml-cards/TopKpis";                   // écoute les deux events

export const TransportDashboard = () => {
  const token =
    typeof window !== "undefined" ? localStorage.getItem("auth_token") ?? "" : "";

  function scrollToAnomalies() {
    const el = document.getElementById("anom-section");
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  return (
    <div className="space-y-6 p-6">
      {/* En-tête */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-primary">Gestion du Transport</h1>
          <p className="text-muted-foreground">Suivi ETA et optimisation transporteurs</p>
        </div>

        {/* ✅ Un seul bouton */}
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
        {/* Colonne gauche : Risque de retard */}
        <DelayRiskCard token={token} />

        {/* Colonne droite : Recommandation + ETA */}
        <div id="prediction-eta" className="flex flex-col gap-6">
          <CarrierSimpleCard token={token} />
          <EtaPredictCard token={token} />
        </div>
      </div>

      {/* ⬇Ancre pour le scroll */}
      <div id="anom-section">
        <AnomalyP90Card token={token} />
      </div>
    </div>
  );
};

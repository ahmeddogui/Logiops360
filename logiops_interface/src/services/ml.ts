// src/services/ml.ts

const API_BASE_URL = "http://127.0.0.1:8000"; // Flask

type FetchOptions = {
  method?: "GET" | "POST";
  token?: string;
  body?: any;
};

/** Appel fetch robuste : tente JSON, sinon montre l’HTML d’erreur (doctype). */
async function callAPI(path: string, { method = "GET", token, body }: FetchOptions = {}) {
  const url = `${API_BASE_URL}${path}`;
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";

  const res = await fetch(url, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let data: any;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    // Réponse non JSON (souvent HTML d’erreur)
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} • ${text.slice(0, 200)}…`);
    }
    // OK sans JSON (rare)
    return {};
  }

  if (!res.ok) {
    throw new Error(data?.message || `HTTP ${res.status}`);
  }
  return data;
}

/* =========================
 *         ETA
 * ========================= */

export async function predictETA(items: any[], token: string) {
  return callAPI("/api/ml/eta/predict", {
    method: "POST",
    token,
    body: { items },
  });
}

export async function predictETAById(shipmentId: string, token: string) {
  const qs = new URLSearchParams({ shipment_id: shipmentId }).toString();
  return callAPI(`/api/ml/eta/predict-by-id?${qs}`, { token });
}

export async function getEtaDistincts(token: string) {
  return callAPI("/api/ml/eta/distincts", { token });
}

export async function getEtaShipmentIds(token: string, limit = 200) {
  const qs = new URLSearchParams({ limit: String(limit) }).toString();
  return callAPI(`/api/ml/eta/shipments?${qs}`, { token }); // { shipment_ids: [...] }
}

/* =========================
 *   RECO TRANSPORTEUR
 * ========================= */

export async function recoSimpleDistincts(token: string) {
  return callAPI("/api/ml/reco-simple/distincts", { token }); // {origin:[], destination_zone:[], service_level:[]}
}

export async function recoSimpleRecommend(payload: any, token: string) {
  return callAPI("/api/ml/reco-simple/recommend", {
    method: "POST",
    token,
    body: payload,
  }); // { best: {...}, topK: [...] }
}

/* =========================
 *   RISQUE DE RETARD
 * ========================= */

export async function delayList(token: string, limit = 40) {
  const qs = new URLSearchParams({ limit: String(limit) }).toString();
  return callAPI(`/api/ml/delay/list?${qs}`, { token }); // { items: [...] }
}

export async function delayDetail(token: string, shipmentId: string) {
  const qs = new URLSearchParams({ shipment_id: shipmentId }).toString();
  return callAPI(`/api/ml/delay/detail?${qs}`, { token }); // {...}
}

/* =========================
 *   ANOMALIES P90 (NOUVEAU)
 * ========================= */

export async function anomP90List(token: string, limit = 30) {
  const qs = new URLSearchParams({ limit: String(limit) }).toString();
  return callAPI(`/api/ml/anom/list?${qs}`, { token }); // { items: [...] }
}

// ... garde le reste inchangé

export async function anomP90Detail(token: string, shipmentId: string, eventId: number) {
  const qs = new URLSearchParams({
    shipment_id: shipmentId,
    event_id: String(eventId),
  }).toString();
  return callAPI(`/api/ml/anom/detail?${qs}`, { token }); // {...}
}

export async function getKpiCounters(token: string) {
  return callAPI("/api/kpi/counters", { token }); // { in_progress: number }
}

"""
H2 – Différences d’erreur d’ETA selon transporteur
Transporteurs réels : GLS, Geodis, DHL, Chrono, UPS
"""

import numpy as np
import pandas as pd
from scipy.stats import kruskal, mannwhitneyu

np.random.seed(2025)

# ------------------------------------------------------------------
# 1) Données simulées : erreur absolue ETA (minutes) par transporteur
# ------------------------------------------------------------------
sizes = {"GLS": 300, "Geodis": 320, "DHL": 310, "Chrono": 290, "UPS": 305}
errs = {
    "GLS":    np.abs(np.random.normal(loc=25, scale=7.0,  size=sizes["GLS"])),    # plutôt fiable
    "Geodis": np.abs(np.random.normal(loc=32, scale=9.0,  size=sizes["Geodis"])), # intermédiaire
    "DHL":    np.abs(np.random.normal(loc=28, scale=8.0,  size=sizes["DHL"])),    # fiable
    "Chrono": np.abs(np.random.normal(loc=36, scale=10.0, size=sizes["Chrono"])), # moins fiable
    "UPS":    np.abs(np.random.normal(loc=34, scale=9.5,  size=sizes["UPS"])),    # moins fiable
}

# Médianes (minutes)
medians = pd.Series({k: float(np.median(v)) for k, v in errs.items()}).sort_values()
print("Médianes d’erreur ETA (minutes) par transporteur :")
print(medians.to_frame("median_min").T)
print()

# ----------------------------------------------------
# 2) Test global Kruskal–Wallis
# ----------------------------------------------------
H, p_kw = kruskal(*errs.values())
print(f"Kruskal–Wallis : H={H:.2f}, p-value={p_kw:.4g}")
decision = "Validée" if p_kw < 0.05 else "❌ Non validée"
print("Décision globale :", decision)
print()

# ------------------------------------------------------------
# 3) Post-hoc Mann–Whitney + correction Benjamini–Hochberg (FDR)
# ------------------------------------------------------------
pairs = [(a,b) for i,a in enumerate(errs.keys()) for b in list(errs.keys())[i+1:]]
pvals = []
for x, y in pairs:
    _, p = mannwhitneyu(errs[x], errs[y], alternative="two-sided")
    pvals.append(p)

# BH correction
m = len(pvals)
order = np.argsort(pvals)
adj = np.empty(m, dtype=float)
min_so_far = 1.0
for rank, i in enumerate(order[::-1], start=1):
    bh = pvals[i] * m / (m - rank + 1)
    min_so_far = min(min_so_far, bh)
    adj[i] = min_so_far

posthoc = pd.DataFrame({
    "pair": [f"{a}-{b}" for a,b in pairs],
    "p_raw": pvals,
    "p_adj_BH": adj
}).sort_values("p_adj_BH")

print("Post-hoc (paires avec correction BH) :")
print(posthoc.to_string(index=False, float_format=lambda x: f"{x:.4g}"))
print()

# ------------------------------------------------------------
# 4) Décision finale et interprétation
# ------------------------------------------------------------
print(f"H2 – Différences d’erreur d’ETA selon transporteur → {decision}")
if decision.startswith("✅"):
    best = medians.index[0]
    worst = medians.index[-1]
    print(f"Interprétation : les différences sont significatives. "
          f"{best} a la médiane d’erreur la plus faible, "
          f"tandis que {worst} est le moins performant.")

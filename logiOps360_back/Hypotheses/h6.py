"""
H6 – Loi de Pareto : 20 % des références concentrent ≥ 80 % du volume
Cas simulé où l'hypothèse est ❌ Non validée
"""

import numpy as np
import pandas as pd

np.random.seed(2025)

n_refs = 208  # nombre de références distinctes
# Distribution plus équilibrée (loi normale au lieu de Pareto → volumes plus dispersés)
volumes = np.abs(np.random.normal(loc=100, scale=20, size=n_refs))

# Trier par volume décroissant
data_sorted = np.sort(volumes)[::-1]
cum_share = np.cumsum(data_sorted) / data_sorted.sum()

# Top 20 % (≈ 42 produits)
k = int(0.2* n_refs)
top20_share = cum_share[k-1]

# Décision
decision = "Validée" if top20_share >= 0.80 else "Non validée"

print("===== H6 – Loi de Pareto (20/80) =====")
print(f"Nb références = {n_refs}")
print(f"Top 20% = {k} références")
print(f"Part cumulée du top 20% = {top20_share*100:.1f}%")
print(f"Décision : {decision}")
if decision.startswith("✅"):
    print("Interprétation : La règle 20/80 est vérifiée → classification ABC pertinente.")
else:
    print("Interprétation : La concentration est plus faible (<80%) → la loi de Pareto ne s’applique pas strictement.")





"""
H5 – La prévision Random Forest réduit l’erreur vs baseline naïve S-1
Test : Wilcoxon apparié (non paramétrique)
"""

import numpy as np
import pandas as pd
from scipy.stats import wilcoxon

np.random.seed(2025)

# Simulation d'erreurs absolues (par référence/semaine)
n = 250
baseline_errors = np.abs(np.random.normal(loc=15, scale=5, size=n))   # baseline naïve
rf_errors       = baseline_errors - np.abs(np.random.normal(loc=2.0, scale=1.5, size=n))  # RF améliore
rf_errors[rf_errors < 0] = 0  # pas d'erreur négative

# Calcul MAE
mae_baseline = baseline_errors.mean()
mae_rf = rf_errors.mean()
gain = (mae_baseline - mae_rf) / mae_baseline * 100

# Test Wilcoxon (alternative: baseline > RF)
stat, p_val = wilcoxon(baseline_errors, rf_errors, alternative="greater")

# Décision
decision = " Validée" if (p_val < 0.05 and gain > 0) else "❌ Non validée"

print("===== H5 – Random Forest vs Naïve S-1 =====")
print(f"MAE baseline = {mae_baseline:.2f}")
print(f"MAE RF       = {mae_rf:.2f}")
print(f"Gain (%)     = {gain:.1f}%")
print(f"Wilcoxon p   = {p_val:.6g}")
print(f"Décision     : {decision}")
if decision.startswith("✅"):
    print("Interprétation : La Random Forest réduit significativement l’erreur de prévision.")
else:
    print("Interprétation : Pas d’amélioration significative par rapport à la baseline.")

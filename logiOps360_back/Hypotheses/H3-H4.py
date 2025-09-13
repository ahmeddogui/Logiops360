"""
H4 – La dispersion augmente la distance de picking
Lecture directe du fichier CSV (résultats Spearman)
Affiche décision (Validée / Non validée)
"""

import pandas as pd
from pathlib import Path

CSV_PATH = Path(r"C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360\Hypotheses\stockage_dispersion_corr.csv")
ALPHA = 0.05

def main():
    df = pd.read_csv(CSV_PATH)
    # On prend la première ligne par convention
    row = df.iloc[0]
    rho = float(row["spearman_rho"])
    pval = float(row["spearman_p"])

    valid = (pval < ALPHA) and (rho > 0)
    decision = "✅ Validée" if valid else "Non validée"

    print("===== H4 – La dispersion augmente la distance de picking =====")
    print(f"Test : Spearman ρ | ρ = {rho:.3f} | p = {pval:.4f}")
    print(f"Décision : {decision}")
    if valid:
        print("Interprétation : Relation positive significative → réduire la dispersion est un levier.")
    else:
        print("Interprétation : Pas de relation positive significative observée dans cet échantillon.")

if __name__ == "__main__":
    main()

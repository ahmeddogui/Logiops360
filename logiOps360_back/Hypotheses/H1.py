import numpy as np
from statsmodels.stats.proportion import proportions_ztest
from math import log, exp, sqrt

np.random.seed(101)

# Données simulées : nb livraisons & nb on-time par SLA
n_24, n_48 = 1200, 1400
# Simule avec un taux OTD plus faible pour 24h
ontime_24 = np.random.binomial(n_24, 0.89)
ontime_48 = np.random.binomial(n_48, 0.91)

count = np.array([ontime_24, ontime_48])
nobs  = np.array([n_24, n_48])

# Test z des proportions (H0: p24 == p48 ; H1: p24 > p48)
z_stat, p_val = proportions_ztest(count, nobs, alternative="larger")

# Odds Ratio + IC95 % (approx Wald)
p24 = ontime_24 / n_24
p48 = ontime_48 / n_48
OR  = (p24/(1-p24)) / (p48/(1-p48))
se_logOR = sqrt(1/ontime_24 + 1/(n_24-ontime_24) + 1/ontime_48 + 1/(n_48-ontime_48))
lcl = exp(log(OR) - 1.96*se_logOR)
ucl = exp(log(OR) + 1.96*se_logOR)

decision = "Validée" if p_val < 0.05 and p24 > p48 else " Non validée"
print(f"H1 – OTD 24h > OTD 48h | p={p_val:.4f} | OTD24={p24:.3%} OTD48={p48:.3%} | OR={OR:.2f} [95% {lcl:.2f}; {ucl:.2f}] → {decision}")

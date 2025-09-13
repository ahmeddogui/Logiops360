import subprocess
import time
import os

print("Lancement du serveur Flask...")
flask_process = subprocess.Popen([
    "python",
    "flask_server.py"
])

time.sleep(5)  # attendre 5 secondes que Flask démarre

print("Lancement du pipeline ETL LogiOps360...")
subprocess.run([
    "python",
    "etl_logiops.py"
])

# Option : arrêter le serveur Flask après l'ETL
print("Arrêt du serveur Flask.")
flask_process.terminate()

print("Processus complet terminé.")

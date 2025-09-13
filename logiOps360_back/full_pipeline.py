import subprocess
import sys
from datetime import datetime
from pathlib import Path

BACK_DIR = Path(__file__).resolve().parent

# Ordre d'exécution
SCRIPTS = [
    BACK_DIR / "ingestion_raw_data.py",
    BACK_DIR / "Commandes" / "Transformations" / "main_transform.py",
    BACK_DIR / "Stockage" / "Transformations" / "main_transform.py",
    BACK_DIR / "Transport" / "Transformations" / "main_transform.py",
]

def run_script(script_path: Path):
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}]  Exécution : {script_path}")
    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}]  Succès : {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}]  Échec : {script_path} (code {e.returncode})")
        sys.exit(e.returncode)

def main():
    print("=== DÉMARRAGE DU FULL PIPELINE LogiOps360 ===")
    for script in SCRIPTS:
        run_script(script)
    print("\n=== FULL PIPELINE TERMINÉ AVEC SUCCÈS ===")

if __name__ == "__main__":
    main()

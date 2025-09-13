import subprocess
import sys
from datetime import datetime

# Liste des scripts à exécuter dans l'ordre
SCRIPTS = [
    r"C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360\ingestion_raw_data.py",
    r"C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360\Commandes\Transformations\main_transform.py",
    r"C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360\Stockage\Transformations\main_transform.py",
    r"C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360\Transport\Transformations\main_transform.py",
]

def run_script(script_path):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  Exécution : {script_path}")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  Succès : {script_path}")
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  Échec : {script_path} (code {e.returncode})")
        sys.exit(e.returncode)

def main():
    print("=== DÉMARRAGE DU FULL PIPELINE LogiOps360 ===")
    for script in SCRIPTS:
        run_script(script)
    print("\n=== FULL PIPELINE TERMINÉ AVEC SUCCÈS ===")

if __name__ == "__main__":
    main()

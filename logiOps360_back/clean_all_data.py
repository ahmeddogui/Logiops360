import sys
import runpy
from pathlib import Path

def run_mod(modname):
    runpy.run_module(modname, run_name="__main__", alter_sys=True)

def main():
    project_root = Path(__file__).resolve().parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    modules = [
        "Commandes.Transformations.main_transform",
        "Transport.Transformations.main_transform",
        "Stockage.Transformations.main_transform"
    ]
    for m in modules:
        print(f"[RUN] {m}")
        run_mod(m)
        print(f"[OK] {m}")

if __name__ == "__main__":
    main()

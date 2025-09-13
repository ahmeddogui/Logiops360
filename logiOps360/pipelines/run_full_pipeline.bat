@echo off
if not exist "..\logs" mkdir "..\logs"
set logFile=..\logs\log_%date:~-4%-%date:~3,2%-%date:~0,2%.txt

echo Lancement du pipeline complet LogiOps360... >> %logFile%
echo Date d'exécution : %date% %time% >> %logFile%
echo. >> %logFile%

cd /d "C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360\pipelines"

"C:\Users\ahmed\AppData\Local\Programs\Python\Python312\python.exe" run_full_pipeline.py >> %logFile% 2>&1

echo. >> %logFile%
echo Fin de l'exécution : %date% %time% >> %logFile%
echo Pipeline exécuté avec succès. Consulte le fichier %logFile% pour les détails.
pause

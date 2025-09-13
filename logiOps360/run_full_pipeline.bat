@echo off
setlocal
chcp 65001 >NUL
set PYTHONUTF8=1

REM === Paramètres projet ===
set "PROJECT_ROOT=C:\Users\ahmed\OneDrive\Bureau\Projet LogiOps360\logiOps360\logiOps360"
set "FULL_PIPELINE=%PROJECT_ROOT%\full_pipeline.py"
set "MAILER=%PROJECT_ROOT%\Transport\send_mail.py"
set "PYTHON=C:\Users\ahmed\AppData\Local\Programs\Python\Python312\python.exe"

REM === Dossier logs (au même niveau que le projet) ===
set "LOGS_DIR=%PROJECT_ROOT%\Logs"
if not exist "%LOGS_DIR%" mkdir "%LOGS_DIR%"
set "RUN_LOG=%LOGS_DIR%\full_pipeline_run_%date:~-4%-%date:~3,2%-%date:~0,2%.txt"

echo [START] %date% %time% - Lancement FULL PIPELINE >> "%RUN_LOG%"
echo Python  : "%PYTHON%" >> "%RUN_LOG%"
echo Script  : "%FULL_PIPELINE%" >> "%RUN_LOG%"
echo. >> "%RUN_LOG%"

REM === Exécuter l’orchestrateur ===
"%PYTHON%" "%FULL_PIPELINE%" >> "%RUN_LOG%" 2>&1
set "ERR=%ERRORLEVEL%"

echo. >> "%RUN_LOG%"
if "%ERR%"=="0" (
  echo [SUCCESS] %date% %time% - Full pipeline termine avec succes. >> "%RUN_LOG%"
  set "SUBJECT=[OK] Full pipeline — donnees a jour"
) else (
  echo [FAIL] %date% %time% - Full pipeline en erreur. Code=%ERR% >> "%RUN_LOG%"
  set "SUBJECT=[FAIL] Full pipeline — erreur (code %ERR%)"
)

REM === Corps de mail dans un fichier (robuste) ===
set "BODY_FILE=%LOGS_DIR%\full_pipeline_mail_body.txt"
> "%BODY_FILE%" echo Rapport FULL PIPELINE - %date% %time%
>> "%BODY_FILE%" echo.
if "%ERR%"=="0" (
  >> "%BODY_FILE%" echo Statut : SUCCES
  >> "%BODY_FILE%" echo Message : Le full pipeline a bien ete execute, les donnees sur la base sont a jour.
) else (
  >> "%BODY_FILE%" echo Statut : ECHEC (code %ERR%)
  >> "%BODY_FILE%" echo Message : Echec du full pipeline. Voir le log ci-joint.
)
>> "%BODY_FILE%" echo.
>> "%BODY_FILE%" echo Log du jour : %RUN_LOG%


REM === Envoi d’email (Gmail SMTP) ===
if exist "%MAILER%" (
  "%PYTHON%" "%MAILER%" ^
    --smtp-host "smtp.gmail.com" --smtp-port 587 ^
    --smtp-user "ahmedbaha.laj@gmail.com" --smtp-pass "evzj icqs vgar foqf" ^
    --from "ahmedbaha.laj@gmail.com" --to "ahmedbaha.laj@gmail.com" ^
    --subject "%SUBJECT%" ^
    --body-file "%BODY_FILE%" ^
    --attach "%RUN_LOG%" >> "%RUN_LOG%" 2>&1
  echo [END] %date% %time% - Email de notification envoye. >> "%RUN_LOG%"
) else (
  echo [WARN] %date% %time% - Mailer introuvable : "%MAILER%". Email non envoye. >> "%RUN_LOG%"
)

endlocal

# Tirer la dernière image ETL et exécuter le pipeline
docker pull ghcr.io/ahmeddogui/logiops_etl_jobs:latest
docker run --rm --env-file .\.env ghcr.io/ahmeddogui/logiops_etl_jobs:latest python full_pipeline.py
if ($LASTEXITCODE -ne 0) { exit 1 } else { exit 0 }

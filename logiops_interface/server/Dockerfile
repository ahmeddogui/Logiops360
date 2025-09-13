FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# ➜ Ajoute ces paquets système requis par sklearn/lightgbm
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 libgfortran5 \
 && rm -rf /var/lib/apt/lists/*

COPY server/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server/ .
EXPOSE 8000
CMD ["gunicorn","-w","2","-k","gthread","-b","0.0.0.0:8000","app:app"]

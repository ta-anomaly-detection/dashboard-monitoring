FROM python:3.11-slim-buster

ENV BACKEND_URI=sqlite:///mlflow.db 

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir mlflow \
    && pip install --no-cache-dir -r requirements.txt

CMD ["mlflow", "ui", "--backend-store-uri", "sqlite:///mlflow.db", "--host", "0.0.0.0"]

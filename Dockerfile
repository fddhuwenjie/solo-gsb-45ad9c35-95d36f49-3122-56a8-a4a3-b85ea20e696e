FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    NAVLIGHT_DB_PATH=/app/data/navlight.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /app/data

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

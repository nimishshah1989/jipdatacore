FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn

COPY app.py .

EXPOSE 8099

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8099"]

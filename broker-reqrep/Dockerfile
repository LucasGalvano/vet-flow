# Broker REQ/REP (ROUTER/DEALER, sem logica de negocio)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY broker_reqrep.py .

# ROUTER (bots) e DEALER (servidores).
EXPOSE 5555 5556

CMD ["python", "broker_reqrep.py"]

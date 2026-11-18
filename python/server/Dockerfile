# Python Server - Parte 1
FROM python:3.11-slim

# Sem isso o Python segura o stdout em buffer quando nao ha terminal (caso do
# Docker) e o `docker compose logs` mostra as linhas com ~15s de atraso.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py persistence.py ./

# Porta interna do socket ZeroMQ REP. O bind real é controlado pela
# variavel de ambiente SERVER_BIND_ADDRESS (default tcp://*:5555, ver
# server.py), portanto o EXPOSE aqui e apenas documentacional.
EXPOSE 5555

# Diretorio de persistencia (montado como volume no docker-compose.yml
# para os dados sobreviverem a um `docker compose down` seguido de
# `docker compose up`).
VOLUME ["/app/data"]

CMD ["python", "server.py"]
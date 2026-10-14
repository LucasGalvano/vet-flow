# Python Client (bot) - Parte 1
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY client.py .

# ACTION, BOT_NAME, SERVER_ADDRESS, CHANNEL_NAME sao definidos no
# docker-compose.yml por servico (cada bot faz UMA acao e sai).
CMD ["python", "client.py"]
# Broker Pub/Sub - Parte 2 (proxy XSUB/XPUB, sem logica de negocio)
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# So o broker vai para a imagem (test_pub.py/test_sub.py sao scripts de
# teste manual e nao precisam estar no container).
COPY broker.py .

# XSUB (publishers/servidores) e XPUB (subscribers/clients).
EXPOSE 5557 5558

CMD ["python", "broker.py"]
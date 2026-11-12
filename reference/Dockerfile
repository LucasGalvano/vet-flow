FROM python:3.11-slim

# Sem isso o Python segura o stdout em buffer quando nao ha terminal (caso do
# Docker) e o `docker compose logs` mostra as linhas com ~15s de atraso.
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY reference.py registry.py ./
EXPOSE 5559
CMD ["python", "reference.py"]

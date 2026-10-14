FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY reference.py registry.py ./
EXPOSE 5559
CMD ["python", "reference.py"]

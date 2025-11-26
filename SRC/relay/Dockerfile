FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY relay.py /app/relay.py

EXPOSE 8000

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/relay.py"]

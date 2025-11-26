FROM python:3.10-slim

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless curl gnupg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

# Copy requirement file and install
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Add the inference code
COPY realtime_inference.py /app/realtime_inference.py

EXPOSE 8080

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/realtime_inference.py"]

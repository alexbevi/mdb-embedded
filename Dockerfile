FROM python:3.11-bookworm

# WiredTiger native build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake swig ninja-build \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mdb_embedded/ mdb_embedded/
COPY templates/ templates/
COPY demo.py web_app.py ./

EXPOSE 5000
CMD ["python", "-u", "web_app.py"]

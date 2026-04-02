FROM python:3.11-bookworm

# WiredTiger native build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake swig ninja-build \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml requirements.txt ./
COPY smongo/ smongo/
RUN pip install --no-cache-dir -e ".[web]"

COPY templates/ templates/
COPY static/ static/
COPY demo.py web_app.py ./

EXPOSE 5000 27018
CMD ["python", "-u", "web_app.py"]

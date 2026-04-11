FROM python:3.11.11-bookworm

# Native libs for compression / common build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake swig ninja-build \
    libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Rust toolchain (needed to compile the PyO3 extension via maturin)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.85.0
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

COPY pyproject.toml requirements.txt ./
COPY rust/ rust/
COPY smongo/ smongo/
COPY LICENSE LICENSE
RUN pip install --no-cache-dir -e ".[web]"

COPY templates/ templates/
COPY static/ static/
COPY demo.py web_app.py ./

EXPOSE 5000 27018
CMD ["python", "-u", "web_app.py"]

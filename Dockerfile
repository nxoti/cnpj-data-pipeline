FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for better caching
COPY requirements/ ./requirements/
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY src/ ./src/
COPY main.py .
COPY setup.py .
COPY validate.py .

# Set environment variables
ENV TEMP_DIR=/app/temp
ENV DATABASE_BACKEND=postgresql
ENV PROCESSING_STRATEGY=auto
ENV BATCH_SIZE=50000
ENV MAX_MEMORY_PERCENT=80
ENV DEBUG=false

# Run the application
ENTRYPOINT ["python", "main.py"]

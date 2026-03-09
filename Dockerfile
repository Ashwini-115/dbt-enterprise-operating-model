# Use official slim Python base
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /dbt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dbt project files into the container
COPY . .

# Default command — can be overridden at runtime
ENTRYPOINT ["dbt"]
CMD ["--help"]
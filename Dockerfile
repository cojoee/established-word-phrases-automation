FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
        && rm -rf /var/lib/apt/lists/*

        # Copy requirements
        COPY requirements.txt .

        # Install Python dependencies
        RUN pip install --no-cache-dir -r requirements.txt

        # Copy application code
        COPY main.py .

        # Create non-root user
        RUN useradd -m -u 1000 automation && chown -R automation:automation /app
        USER automation

        # Health check
        HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
            CMD python main.py health || exit 1

            # Run the application
            CMD ["python", "main.py"]

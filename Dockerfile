FROM apache/spark:3.5.0-python3

# Install additional Python dependencies
USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
# COPY . /app
WORKDIR /app

# Make entrypoint script executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose port
EXPOSE 8000
EXPOSE 8082

# Use entrypoint script
ENTRYPOINT ["./entrypoint.sh"]
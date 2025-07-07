#!/bin/bash
set -e

echo "=== Starting Luigi scheduler ==="
# Start luigid in background
luigid &

# Run Luigi pipeline
echo "Starting Luigi pipeline..."
python3 -m luigi --module pipeline.tasks.task_ge_check DataCheckTask \
    --scheduler-host localhost \
    --scheduler-port 8082

# Check if pipeline completed successfully
if [ $? -eq 0 ]; then
    echo "Luigi pipeline completed successfully"
    
    # Start the FastAPI application
    echo "Starting FastAPI application..."
    uvicorn api.main:app --host 0.0.0.0 --port 8000
else
    echo "Luigi pipeline failed"
    exit 1
fi
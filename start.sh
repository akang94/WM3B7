#!/bin/bash
echo "Starting environment..."

# Activate virtual environment
source /workspaces/WM3B7/.venv/bin/activate

# Clean restart Kafka (prevents stale Zookeeper issue)
cd /workspaces/WM3B7/kafka_tutorial
docker-compose down
echo "Waiting for clean shutdown..."
sleep 5
docker-compose up -d
echo "Waiting for Kafka to start..."
sleep 20

# Start Airflow
export AIRFLOW_HOME=/workspaces/WM3B7/airflow
rm -f /workspaces/WM3B7/airflow/airflow-webserver.pid
cd /workspaces/WM3B7
airflow webserver --port 8080 -D
sleep 10
airflow scheduler -D

echo "✅ Everything is running!"
echo "👉 Open port 8080 in the PORTS tab for Airflow UI"
echo "👉 Kafka is on localhost:29092"

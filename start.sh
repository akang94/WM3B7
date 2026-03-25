cat > /workspaces/WM3B7/start.sh << 'EOF'
#!/bin/bash
echo "Starting environment..."

# Activate virtual environment
source /workspaces/WM3B7/.venv/bin/activate

# Start Kafka
cd /workspaces/WM3B7/kafka_tutorial
docker-compose up -d
echo "Waiting for Kafka to start..."
sleep 15

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
EOF
chmod +x /workspaces/WM3B7/start.sh
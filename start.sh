#!/bin/bash
echo "🚀 Starting WM3B7 ETL environment..."

# Activate virtual environment
source /workspaces/WM3B7/.venv/bin/activate

# Clean restart Kafka
echo "Starting Kafka..."
cd /workspaces/WM3B7/kafka_tutorial
docker-compose down
sleep 5
docker-compose up -d
echo "Waiting for Kafka to be ready..."
sleep 20

# Recreate Kafka topics (safe if they already exist)
echo "Creating Kafka topics..."
docker exec broker kafka-topics --create \
  --topic coinbase_ticker \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || echo "coinbase_ticker already exists"

docker exec broker kafka-topics --create \
  --topic coinbase_matches \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1 2>/dev/null || echo "coinbase_matches already exists"

# Start Airflow
echo "Starting Airflow..."
export AIRFLOW_HOME=/workspaces/WM3B7/airflow
rm -f /workspaces/WM3B7/airflow/airflow-webserver.pid

# Reinitialise DB if needed
airflow db init 2>/dev/null

# Copy DAG to Airflow folder
cp /workspaces/WM3B7/dags/coinbase_etl_dag.py /workspaces/WM3B7/airflow/dags/

# Start webserver and scheduler
airflow webserver --port 8080 -D
sleep 10
airflow scheduler -D
sleep 10

# Unpause and trigger the DAG
echo "Unpausing and triggering DAG..."
airflow dags unpause coinbase_etl_pipeline
sleep 3
airflow dags trigger coinbase_etl_pipeline

echo ""
echo "✅ Everything is running!"
echo "👉 Open port 8080 in the PORTS tab for Airflow UI"
echo "👉 Kafka is on localhost:29092"
echo "👉 DAG has been triggered - check the UI to watch it run!"

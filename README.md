# Online Shop - How to deploy from scratch

### 1. Requirements

Docker Desktop and Docker Compose v2.

Python 3.10+ (only required to run the Kafka consumer locally).

Internet connection for downloading Docker images and Spark dependencies during the initial build.

### 2. Start the project

From the project folder:

```powershell
docker compose up --build -d
```

Docker automatically starts PostgreSQL, Kafka, Debezium, Schema Registry, Spark and MinIO. It also:

- Loads the three CSV datasets into PostgreSQL.
- Registers the JSON schemas and Debezium connector.
- Creates the MinIO `lake` bucket.
- Starts Spark streaming and saves processed events in Delta format.

*The following sections contain verification commands, reliability tests and optional environment reset instructions. None of these are required for the initial deployment.*

### 3. Verify deployment

```powershell
docker compose ps -a
docker compose logs loader setup minio-init
```

The main services should be running, while initialization services should finish with `Exited (0)`.

#### Verify Debezium and Kafka

```powershell
Invoke-RestMethod http://localhost:8083/connectors/online-shop-connector/status | ConvertTo-Json -Depth 5
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
Invoke-RestMethod http://localhost:8081/subjects
```

The connector should be RUNNING, and all three online shop topics and their registered JSON schemas should be available.

#### Run the Kafka consumer

Open a separate terminal in the project folder:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r scripts/requirements.txt
python scripts/consumer.py
```

The consumer reads JSON messages from all three topics and validates them against the schemas retrieved from Schema Registry.

Stop the consumer with Ctrl+C.

#### Spark output:

```powershell
docker compose logs --tail=70 spark-app
```

Before retrieving the Delta data, check that Spark has started processing the initial order events.

#### Read the Delta data saved in MinIO:

```powershell
docker compose exec spark-app /opt/spark/bin/spark-submit --master "local[2]" /opt/spark-app/read_delta.py
```

On a fresh deployment, the script should display five initial order events and `Total rows: 5`.

#### Test a new database change

```powershell
docker compose exec postgres psql -U shop -d shopdb
```

Inside PostgreSQL:

```sql
UPDATE orders
SET status = 'shipped', currency = 'eur'
WHERE order_id = 1005;

\q
```

Check the Spark output:

```powershell
docker compose logs --tail=70 spark-app
```

The new event should appear with status SHIPPED and currency EUR.

The Delta table stores order change events, including snapshots, inserts, updates and deletes, rather than only the latest state of each order.

### 4. Reliability

Docker volumes preserve PostgreSQL, Kafka, MinIO and Spark checkpoint data. Health checks, startup dependencies and restart policies help services start in the correct order and recover from failures.

Restart Spark and check that the Delta data is still available:

```powershell
docker compose restart spark-app
docker compose ps -a
docker compose logs --tail=70 spark-app
```

```powershell
docker compose exec spark-app /opt/spark/bin/spark-submit --master "local[2]" /opt/spark-app/read_delta.py
```

With no new events, the Delta row count should remain the same.

Restart the whole project:

```powershell
docker compose down
docker compose up -d
```

The existing data should remain available. Do not use `docker compose down -v` because it deletes the stored volumes.

### 5. Reset the environment (optional)

To remove all existing data and deploy from scratch:

```powershell
docker compose down -v
docker compose up --build -d
```

Warning: this deletes all stored data, Kafka history and Spark checkpoints.